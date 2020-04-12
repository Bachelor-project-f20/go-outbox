package outbox_test

import (
	"fmt"
	"testing"

	"github.com/Bachelor-project-f20/go-outbox"
	models "github.com/Bachelor-project-f20/shared/models"
)

type testSchema struct {
	ID   string
	Name string
	Age  int32
}

type mockEmitter struct{}

func (m mockEmitter) Emit(e models.Event) error {
	fmt.Println("Emitting event: ", e.ID)
	return nil
}

func TestNewOutbox(t *testing.T) {
	_, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", 10, mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}
}

func TestInsert(t *testing.T) {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", 10, mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}
	event := models.Event{}
	event.ID = "ID-01-IN"
	event.EventName = "pub"
	event.Publisher = "name"
	event.Timestamp = 23
	event.Payload = []byte("hello")
	err = db.Insert(testSchema{"1", "Bob", 29}, event)
	if err != nil {
		t.Error(err)
	}

	event.ID = "ID-02-IN"
	err = db.Insert(testSchema{"2", "Bob", 29}, event)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", 10, mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	event := models.Event{}
	event.ID = "ID-02-UP"
	event.EventName = "pub"
	event.Publisher = "name"
	event.Timestamp = 23
	event.Payload = []byte("hello")
	err = db.Update(testSchema{"1", "Bob", 30}, event)
	if err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", 10, mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	event := models.Event{}
	event.ID = "ID-03-DE"
	event.EventName = "pub"
	event.Publisher = "name"
	event.Timestamp = 23
	event.Payload = []byte("hello")
	err = db.Delete(testSchema{"1", "Bob", 30}, event)
	if err != nil {
		t.Error(err)
	}
}
