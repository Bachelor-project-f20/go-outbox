package outbox_test

import (
	"testing"

	"github.com/dueruen/go-outbox"
)

var db outbox.Outbox

type testSchema struct {
	ID   string
	Name string
	Age  int32
}

func TestNewOutbox(t *testing.T) {
	out, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", testSchema{})
	if err != nil {
		t.Error(err)
	}
	db = out
}

func TestInsert(t *testing.T) {
	err := db.Insert(testSchema{"1", "Bob", 29}, outbox.Event{"id", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	err := db.Update(testSchema{"1", "Bob", 30}, outbox.Event{"id2", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	err := db.Delete(testSchema{"1", "Bob", 30}, outbox.Event{"id3", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}
