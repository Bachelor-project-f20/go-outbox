package outbox_test

import (
	"fmt"
	"testing"

	"github.com/dueruen/go-outbox"
)

var db outbox.Outbox
var rel outbox.Relay

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

func TestNewRelay(t *testing.T) {
	r, err := outbox.NewRelay(db.GetDBConnection())
	if err != nil {
		t.Error(err)
	}
	rel = r
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

func TestListen(t *testing.T) {
	err := rel.ListenForTesting(5, func(e []outbox.Event) bool {
		if len(e) == 0 {
			fmt.Println("No events, oh no")
			t.Error()
			return false
		}
		fmt.Println("found Event: ", e[0].ID)
		return true
	})

	if err != nil {
		t.Error(err)
	}
}
