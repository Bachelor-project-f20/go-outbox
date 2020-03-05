package main

import (
	"github.com/dueruen/go-outbox"
)

type testSchema struct {
	ID   string
	Name string
	Age  int32
}

func main() {
	db, err := outbox.NewOutbox(outbox.MySQL, "root:root@/root?charset=utf8&parseTime=True&loc=Local", mockEmitter{}, testSchema{})
	if err != nil {
		panic(err)
	}

	err = db.Insert(testSchema{"main1", "Bob", 29}, outbox.Event{"mainID1", "pub", "name", 23, []byte("hello")})
	if err != nil {
		panic(err)
	}

	for {

	}
}

type mockEmitter struct{}

func (m mockEmitter) Emit(e outbox.Event) error {
	return nil
}
