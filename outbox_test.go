package outbox_test

import (
	"fmt"
	"testing"

	"github.com/dueruen/go-outbox"
)

type testSchema struct {
	ID   string
	Name string
	Age  int32
}

type mockEmitter struct{}

func (m mockEmitter) Emit(e outbox.Event) error {
	fmt.Println("Emitting event: ", e.ID)
	return nil
}

func TestNewOutbox(t *testing.T) {
	_, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}
}

func TestInsert(t *testing.T) {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	err = db.Insert(testSchema{"1", "Bob", 29}, outbox.Event{"ID-01-IN", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	err = db.Update(testSchema{"1", "Bob", 30}, outbox.Event{"ID-02-UP", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	err = db.Delete(testSchema{"1", "Bob", 30}, outbox.Event{"ID-03-DE", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}
