// Code generated by "stringer -type=DataType"; DO NOT EDIT

package eav

import "fmt"

const _DataType_name = "undefStringNumberBooleanTime"

var _DataType_index = [...]uint8{0, 5, 11, 17, 24, 28}

func (i DataType) String() string {
	if i < 0 || i >= DataType(len(_DataType_index)-1) {
		return fmt.Sprintf("DataType(%d)", i)
	}
	return _DataType_name[_DataType_index[i]:_DataType_index[i+1]]
}
