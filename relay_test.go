package outbox_test

import (
	"testing"

	"github.com/Bachelor-project-f20/go-outbox"
	models "github.com/Bachelor-project-f20/shared/models"
)

func TestNewRelay(t *testing.T) {
	out, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", 10, mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	event := models.Event{}
	event.ID = "Relay01"
	event.EventName = "pub"
	event.Publisher = "name"
	event.Timestamp = 23
	event.Payload = []byte("hello")
	err = out.Insert(testSchema{"r1", "Bob", 29}, event)
	if err != nil {
		t.Error(err)
	}

	event.ID = "Relay02"
	err = out.Insert(testSchema{"r2", "Bob", 29}, event)
	if err != nil {
		t.Error(err)
	}
}
