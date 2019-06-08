package eav

import (
	"database/sql"
	"fmt"
)

type Handle interface {
	Exec(text string, params ...interface{}) (sql.Result, error)
	Query(text string, params ...interface{}) (*sql.Rows, error)
	Prepare(text string) (*sql.Stmt, error)
}

func InitTables(db Handle) error {
	_, err := db.Exec(`create table if not exists eav_schema (
		store_id bigint,
		datatype int,
		name text,
		primary key (store_id, name)
	)`)
	if err != nil {
		return fmt.Errorf("initializing schema table: %s", err)
	}
	_, err = db.Exec(`create table if not exists eav_datoms (
		store_id bigint,
		entity_id text not null,
		attribute_name text not null,
		stringval text null,
		numberval numeric null,
		booleanval bool null,
		timeval timestamptz null,
		primary key (store_id, entity_id, attribute_name),
		foreign key (store_id, attribute_name) references eav_schema(store_id, name) on delete cascade
	)`)
	if err != nil {
		return fmt.Errorf("initializing datoms table: %s", err)
	}
	return nil
}
