package eav

import (
	"database/sql"
	"fmt"
	"time"
)

type Datom struct {
	entity        interface{}
	attributeName string
	attributeID   uint32
	dataType      DataType
	stringVal     sql.NullString
	numberVal     sql.NullFloat64
	booleanVal    sql.NullBool
	timeVal       NullTime
}

func (row Datom) Value() interface{} {
	switch row.dataType {
	case Number:
		return row.numberVal.Float64
	case String:
		return row.stringVal.String
	case Boolean:
		return row.booleanVal.Bool
	case Time:
		return row.timeVal.Time
	}
	return nil
	// panic(fmt.Sprintf("Datom has unknown data type %s", row.dataType))
}

func (row *Datom) setValue(val interface{}) error {
	switch row.dataType {
	case String:
		if s, ok := val.(string); ok {
			row.stringVal = sql.NullString{s, true}
			return nil
		}
	case Boolean:
		if b, ok := val.(bool); ok {
			row.booleanVal = sql.NullBool{b, true}
			return nil
		}
	case Time:
		if t, ok := val.(time.Time); ok {
			row.timeVal = NullTime{t, true}
			return nil
		}
	case Number:
		var f float64
		switch v := val.(type) {
		case float64:
			f = v
		case float32:
			f = float64(v)
		case int:
			f = float64(v)
		case uint32:
			f = float64(v)
		case uint64:
			f = float64(v)
		case int32:
			f = float64(v)
		case int64:
			f = float64(v)
		default:
			return badType(val, row.dataType)
		}
		row.numberVal = sql.NullFloat64{f, true}
		return nil
	}
	return badType(val, row.dataType)
}

func badType(val interface{}, dataType DataType) error {
	return fmt.Errorf(`Cannot assign %T to %s`, val, dataType)
}
