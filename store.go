package eav

import (
	"database/sql"
	"fmt"
	// "strings"
)

type Schema map[string]Attribute

type Store struct {
	db          Handle
	storeID     uint32
	assertStmt  *sql.Stmt
	retractStmt *sql.Stmt
}

// Initialize a new eav.Store with the given id
func New(db Handle, storeID uint32) *Store {
	return &Store{db: db, storeID: storeID}
}

func (store *Store) Schema() (map[string]Attribute, error) {
	query := `
		select name, datatype
		from eav_schema
		where store_id = $1`
	result, err := store.db.Query(query, store.storeID)
	if err != nil {
		return nil, err
	}
	attrs := map[string]Attribute{}
	for result.Next() {
		attr := Attribute{storeID: store.storeID}
		err := result.Scan(&attr.Name, &attr.DataType)
		if err != nil {
			return nil, err
		}
		attrs[attr.Name] = attr
	}
	return attrs, err
}

// Define a new attribute. This call is idempotent and can be repeated safely, but
// will fail if the attribute was already defined with a different type.
func (store *Store) DefineAttribute(name string, dataType DataType) (Attribute, error) {
	query := `insert into eav_schema (store_id, name, datatype)
		values ($1, $2, $3)
		on conflict (store_id, name) do update set datatype = EXCLUDED.datatype
		returning datatype`
	rows, err := store.db.Query(query, store.storeID, name, dataType)
	if err != nil {
		return Attribute{}, err
	}
	attr := Attribute{
		storeID: store.storeID,
		Name:    name,
	}
	for rows.Next() {
		err := rows.Scan(&attr.DataType)
		if err != nil {
			return Attribute{}, err
		}
	}

	if attr.DataType != dataType {
		return Attribute{}, fmt.Errorf(
			"Cannot change datatype of %q attribute from %s to %s",
			attr.Name, attr.DataType, dataType,
		)
	}

	return attr, err
}

// Completely remove an attribute and any associated values
func (store *Store) ForgetAttribute(name string) error {
	_, err := store.db.Exec(
		`delete from eav_schema where store_id = $1 and name = $2`,
		store.storeID, name,
	)
	return err
}

type EntityId interface{}

type Attributes map[string]interface{}

// Returns the defined attributes for an entity
func (store *Store) Attributes(eId EntityId) (Attributes, error) {
	rows, err := store.readDatoms(eId)
	attributes := make(map[string]interface{}, len(rows))

	if err == sql.ErrNoRows {
		return attributes, nil
	} else if err != nil {
		return nil, err
	}

	for _, row := range rows {
		attributes[row.attributeName] = row.Value()
	}

	return attributes, nil
}

// Update an entities attributes by merging. Any conflicting attributes will be
// replaced. If an attribute value is explicitly set to nil, that attribute will
// be removed.
func (store *Store) Update(eId EntityId, newAttrs Attributes) (Attributes, error) {
	schemas, err := store.Schema()
	if err != nil {
		return nil, err
	}

	datoms, err := store.readDatoms(eId)
	if err != nil {
		return nil, err
	}

	datomMap := make(map[string]Datom, len(datoms))
	attrs := make(Attributes)

	for _, datom := range datoms {
		datomMap[datom.attributeName] = datom
		attrs[datom.attributeName] = datom.Value()
	}

	assertions := []Datom{}
	retractions := []Datom{}

	for attrName, val := range newAttrs {
		schema, defined := schemas[attrName]

		if !defined {
			return nil, fmt.Errorf(`Attribute "%s" is not defined`, attrName)
		}

		oldDatom, isUpdate := datomMap[attrName]

		if val == nil {
			if isUpdate {
				retractions = append(retractions, oldDatom)
			}
			delete(attrs, attrName)
			continue
		}

		datom := Datom{
			entity:        eId,
			attributeName: attrName,
			dataType:      schema.DataType,
		}

		err := datom.setValue(val)

		if err != nil {
			return nil, fmt.Errorf(`%s attribute "%s"`, err, attrName)
		}

		attrs[attrName] = datom.Value()

		assertions = append(assertions, datom)
	}

	if len(retractions) > 0 {
		_ = "delete from eav_datoms where attribute in (" + "...datoms..." + ")"
		if err = store.Retract(retractions...); err != nil {
			return nil, err
		}
	}
	if len(assertions) > 0 {
		if err = store.Assert(assertions...); err != nil {
			return nil, err
		}
	}
	return attrs, nil
}

// Assert inserts attributes into the EAV store
func (store *Store) Assert(assertions ...Datom) error {
	if store.assertStmt == nil {
		var err error
		store.assertStmt, err = store.db.Prepare(`
		  insert into eav_datoms
		  (store_id, entity_id, attribute_name, stringval, numberval, booleanval, timeval)
		  values ($1, $2, $3, $4, $5, $6, $7)
		  on conflict (store_id, entity_id, attribute_name) do update set
			(stringval, numberval, booleanval, timeval) = ($4, $5, $6, $7)`)

		if err != nil {
			return err
		}
	}

	for _, assertion := range assertions {
		_, err := store.assertStmt.Exec(
			store.storeID,
			assertion.entity,
			assertion.attributeName,
			assertion.stringVal,
			assertion.numberVal,
			assertion.booleanVal,
			assertion.timeVal,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (store *Store) Retract(retractions ...Datom) (err error) {
	if store.retractStmt == nil {
		store.retractStmt, err = store.db.Prepare(
			`delete from eav_datoms
			 where store_id = $1 and entity_id = $2 and attribute_name = $3`)
		if err != nil {
			return
		}
	}
	for _, datom := range retractions {
		_, err = store.retractStmt.Exec(store.storeID, datom.entity, datom.attributeName)
	}
	return err
}

func (store *Store) ForgetEntity(id interface{}) error {
	datoms, err := store.readDatoms(id)
	if err != nil {
		return err
	}
	return store.Retract(datoms...)
}

func (store *Store) readDatoms(eId interface{}) ([]Datom, error) {
	query := `select
	  datoms.stringval,
	  datoms.numberval,
	  datoms.booleanval,
	  datoms.timeval,
	  s.datatype,
	  s.name
	from eav_datoms datoms
	  join eav_schema s on datoms.attribute_name = s.name and s.store_id = $1
	where
	  datoms.store_id = $1 and datoms.entity_id = $2`

	result, err := store.db.Query(query, store.storeID, eId)
	if err != nil {
		return nil, fmt.Errorf("read datoms: %s", err)
	}
	var datoms []Datom
	for result.Next() {
		d := Datom{entity: eId}
		err := result.Scan(&d.stringVal, &d.numberVal, &d.booleanVal, &d.timeVal, &d.dataType, &d.attributeName)
		if err != nil {
			return nil, err
		}
		datoms = append(datoms, d)
	}
	return datoms, err
}
