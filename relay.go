package outbox

import (
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
)

var cache map[string]struct{}

type Relay interface {
	Listen(pollWait time.Duration, eventHandler func(e []Event)) error
}

func NewRelay(db *gorm.DB) (Relay, error) {
	return &Storage{db: db}, nil
}

func (s *Storage) Listen(pollWait time.Duration, eventHandler func(e []Event)) error {
	var events []Event
	for {
		fmt.Println("Polling")
		time.Sleep(pollWait * time.Second)
		s.db.Find(&events) //This is obviously not ideal, scales like pure garbage
		fmt.Printf("Found events: %v", events)

		newEvents := checkEvents(events)
		eventHandler(newEvents)
	}
}

func checkEvents(events []Event) []Event {
	var newEvents []Event
	for _, event := range events {
		if !cacheContains(event.ID) {
			newEvents = append(newEvents, event)
		}
	}
	return newEvents
}

func cacheContains(eventID string) bool {
	_, ok := cache[eventID]
	return ok
}
