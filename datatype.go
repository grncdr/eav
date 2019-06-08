package eav

//go:generate stringer -type=DataType
type DataType uint8

const (
	undef DataType = iota
	String
	Number
	Boolean
	Time
)
