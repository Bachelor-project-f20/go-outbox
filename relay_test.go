package outbox_test

import (
	"testing"

	etg "github.com/Bachelor-project-f20/eventToGo"
	"github.com/Bachelor-project-f20/go-outbox"
)

func TestNewRelay(t *testing.T) {
	out, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	err = out.Insert(testSchema{"r1", "Bob", 29}, etg.Event{"Relay01", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}

	err = out.Insert(testSchema{"r2", "Bob", 29}, etg.Event{"Relay02", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}
}
