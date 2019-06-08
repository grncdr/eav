package eav

type Attribute struct {
	storeID  uint32
	id       uint32
	Name     string
	DataType DataType
}

func (attr Attribute) Datom(entity EntityId, value interface{}) {
	d := Datom{
		attributeID:   attr.id,
		entity:        entity,
		dataType:      attr.DataType,
		attributeName: attr.Name,
	}
	d.setValue(value)
}
