package eav

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB

func setup(storeIds ...uint32) (map[uint32]*Store, func()) {
	txn, err := db.Begin()
	if err != nil {
		panic(err)
	}
	stores := make(map[uint32]*Store)
	for _, id := range storeIds {
		stores[id] = New(txn, id)
	}
	return stores, func() {
		err := txn.Rollback()
		if err != nil {
			panic(err)
		}
	}
}

func TestMain(m *testing.M) {
	var err error
	db, err = sql.Open("postgres", "dbname=eav_test sslmode=disable")
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}
	err = InitTables(db)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}
	os.Exit(m.Run())
}

func TestSmoke(t *testing.T) {
	stores, rollback := setup(1)
	defer rollback()

	_, err := stores[1].DefineAttribute("count", Number)
	if err != nil {
		t.Fatal(err)
	}

	eId := "abcdefg"
	attrs, err := stores[1].Attributes(eId)
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) != 0 {
		t.Fatal("Expected empty attrs map")
	}

	attrs, err = stores[1].Update(eId, Attributes{"count": 359})
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) != 1 {
		t.Fatal("Expected 1 attribute")
	}

	attrsʹ, err := stores[1].Attributes(eId)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(attrsʹ, attrs) {
		t.Fatalf("Expected %s to equal %s", attrsʹ, attrs)
	}

	err = stores[1].ForgetAttribute("count")
	if err != nil {
		t.Fatal(err)
	}
	attrsʹ, err = stores[1].Attributes(eId)
	if err != nil {
		t.Fatal(err)
	} else if len(attrsʹ) > 0 {
		t.Fatalf("Expected attrs to be empty: %s", attrsʹ)
	}
}

func TestStoreIsolation(t *testing.T) {
	stores, rollback := setup(1, 2)
	defer rollback()
	_, err := stores[1].DefineAttribute("country", String)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stores[2].DefineAttribute("country", Number)
	if err != nil {
		t.Fatal(err)
	}

	// first store has string countries
	_, err = stores[1].Update("thing", Attributes{
		"country": "Kanada",
	})
	if err != nil {
		t.Fatal(err)
	}

	// second store has numeric countries
	_, err = stores[2].Update("thing", Attributes{
		"country": 49,
	})
	if err != nil {
		t.Fatal(err)
	}

	// expect a type error
	_, err = stores[1].Update("thing", Attributes{
		"country": 49,
	})
	if err == nil {
		t.Fatal("Expected badType error")
	} else if err.Error() != `Cannot assign int to String attribute "country"` {
		t.Fatal("Unexpected error message:", err.Error())
	}

	// destroy attribute in first store
	err = stores[1].ForgetAttribute("country")
	if err != nil {
		t.Fatal(err)
	}

	// make sure it's actually gone
	attrs, err := stores[1].Attributes("thing")
	if err != nil {
		t.Fatal(err)
	} else if len(attrs) > 0 {
		t.Fatalf("Expected %s to be empty after ForgetAttribute", attrs)
	}

	// ensure that the other store is not affected
	attrs, err = stores[2].Attributes("thing")
	expected := Attributes{"country": float64(49)}
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(attrs, expected) {
		t.Fatalf("Expected %s to equal %s", attrs, expected)
	}

	attrs, err = stores[2].Update("thing", Attributes{
		"country": 32,
	})
	if err != nil {
		t.Fatal(err)
	} else if attrs["country"].(float64) != 32 {
		t.Fatalf("Expected country to equal 32: %s", attrs)
	}
}

func TestDataTypes(t *testing.T) {
	eavStores, rollback := setup(1)
	eavStore := eavStores[1]
	defer rollback()

	_, err := eavStore.DefineAttribute("someTime", Time)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eavStore.DefineAttribute("regionCode", String)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eavStore.DefineAttribute("maxBudget", Number)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eavStore.DefineAttribute("exclusive", Boolean)
	if err != nil {
		t.Fatal(err)
	}

	setAndVerify := func(eId EntityId, expected Attributes) {
		attrs, err := eavStore.Update(eId, expected)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(attrs, expected) {
			t.Fatalf("Expected %s to equal %s after Update", attrs, expected)
		}

		attrs, err = eavStore.Attributes(eId)
		if err != nil {
			t.Fatal(err)
		}
		for name, value := range expected {
			var equal bool
			if name == "someTime" {
				equal = value.(time.Time).Equal(attrs[name].(time.Time))
			} else {
				equal = reflect.DeepEqual(attrs[name], value)
			}
			if !equal {
				t.Fatalf("Expected %s %s to equal %s after Attributes", name, attrs[name], value)
			}
		}
	}

	setAndVerify("first", Attributes{
		"regionCode": "V8Z3T3",
		"maxBudget":  float64(124561.36),
		"exclusive":  true,
		"someTime":   time.Now().Truncate(time.Millisecond).AddDate(0, 1, 0),
	})

	setAndVerify("first", Attributes{
		"regionCode": "V83ZT3",
		"maxBudget":  float64(1261.6),
		"exclusive":  false,
		"someTime":   time.Now().Truncate(time.Millisecond).AddDate(0, 1, 1),
	})
}

func TestRemoveAttributes(t *testing.T) {
	eavStores, rollback := setup(1)
	defer rollback()
	s := eavStores[1]
	_, err := s.DefineAttribute("label", String)
	_, err = s.DefineAttribute("other", String)
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := s.Update(1, Attributes{
		"label": "whatever",
	})
	if err != nil {
		t.Fatal(err)
	}
	attrs, err = s.Attributes(1)
	if err != nil {
		t.Fatal(err)
	} else if attrs["label"].(string) != "whatever" {
		t.Fatal("expected attr 'blah' to be 'whatever'")
	}

	attrs, err = s.Update(1, Attributes{
		"label": nil,
		"other": "hi",
	})

	expect := Attributes{"other": "hi"}
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(attrs, expect) {
		t.Fatalf("Expected %s to equal %s", attrs, expect)
	}

	attrs, err = s.Attributes(1)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(attrs, expect) {
		t.Fatalf("Expected %s to equal %s", attrs, expect)
	}
}

func TestSetUndefinedAttribute(t *testing.T) {
	stores, rollback := setup(1)
	defer rollback()
	_, err := stores[1].Update(1, Attributes{
		"non-existant": "ok",
	})
	if err == nil {
		t.Fatal("expected error setting non-existant attribute")
	} else if err.Error() != `Attribute "non-existant" is not defined` {
		t.Fatal("error when setting non-existant attribute was wrong", err)
	}
}

func TestForgettingEntities(t *testing.T) {
	eavStores, rollback := setup(1)
	defer rollback()
	s := eavStores[1]
	var err error
	_, err = s.DefineAttribute("rrr", String)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DefineAttribute("sss", String)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Update("thing/1", Attributes{
		"rrr": "ok",
		"sss": "blah",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.ForgetEntity("thing/1")
	if err != nil {
		t.Fatal(err)
	}

	attrs, err := s.Attributes("thing/1")
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(attrs, Attributes{}) {
		t.Fatal("Expected empty attribute set after ForgetEntity")
	}
}
