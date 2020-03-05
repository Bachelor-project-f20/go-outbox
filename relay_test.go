package outbox_test

import (
	"testing"
	"time"

	"github.com/dueruen/go-outbox"
)

var rel outbox.Relay

func TestNewRelay(t *testing.T) {
	out, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}

	err = out.Insert(testSchema{"r1", "Bob", 29}, outbox.Event{"Relay01", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}

	err = out.Insert(testSchema{"r2", "Bob", 29}, outbox.Event{"Relay02", "pub", "name", 23, []byte("hello")})
	if err != nil {
		t.Error(err)
	}

	r, err := outbox.NewRelay(out.GetDBConnection())
	if err != nil {
		t.Error(err)
	}
	rel = r
}

func TestListen(t *testing.T) {
	out, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		t.Error(err)
	}
	r, err := outbox.NewRelay(out.GetDBConnection())
	if err != nil {
		t.Error(err)
	}

	r.ListenDebug(5, 5, mockEmitter{}, true)
	time.Sleep(3 * time.Second)

	if err != nil {
		t.Error(err)
	}
}
