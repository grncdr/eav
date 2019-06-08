package eav

import (
	"testing"
)

func TestNilDatom(t *testing.T) {
	d := Datom{}
	if d.Value() != nil {
		t.Error("Empty datom had non-nil value", d.Value())
	}
}

func TestNumberCoercion(t *testing.T) {
	d := Datom{dataType: Number}
	ones := []interface{}{
		int(1),
		int32(1),
		int64(1),
		uint32(1),
		uint64(1),
		float32(1),
		float64(1),
	}

	for _, one := range ones {
		err := d.setValue(one)
		if err != nil {
			t.Error(err)
		} else if d.Value() != float64(1) {
			t.Errorf("%t was not converted to float64", one)
		}
	}

	err := d.setValue("wat")
	expectedErr := "Cannot assign string to Number"
	if err == nil {
		t.Error("Expected error when setting value of number datom to string")
	} else if err.Error() != expectedErr {
		t.Errorf("Unexpected error when setting number datom to string. Expected %q, got %q", expectedErr, err.Error())
	}
}
