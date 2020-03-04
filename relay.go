package outbox

import (
	"errors"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
)

var cache = make(map[string]struct{}) //perhaps not necessary?

type Relay interface {
	Listen(pollWait time.Duration, eventHandler func(e []Event) bool) error
	ListenForTesting(pollWait time.Duration, eventHandler func(e []Event) bool) error
}

func NewRelay(db *gorm.DB) (Relay, error) {
	return &Storage{db: db}, nil
}

func (s *Storage) Listen(pollWait time.Duration, eventHandler func(e []Event) bool) error {
	var events []Event
	for {
		fmt.Println("Polling")

		s.db.Find(&events) //This is obviously not ideal, scales like pure garbage - or does it, now that we clear the outbox?
		if len(events) == 0 {
			time.Sleep(pollWait * time.Second)
			continue
		}

		fmt.Printf("Found events: %v", events)

		newEvents := checkEvents(events) //remove already handled events
		handled := eventHandler(newEvents)

		if !handled { //handler function returns false, sleep 1 sec, retry
			time.Sleep(1 * time.Second)
			continue
		}

		s.ClearOutbox(events) //if handler functions returns true, clear the outbox of the handled events
		addToCache(newEvents)
		time.Sleep(pollWait * time.Second)
	}
}

//Without the infinite loop, so that it can actually be tested
//I realise that this is less than ideal, just a quick fix to not push failing code to Github (suggested by random dude on StackOverflow)
func (s *Storage) ListenForTesting(pollWait time.Duration, eventHandler func(e []Event) bool) error {
	var events []Event
	fmt.Println("Polling")

	s.db.Find(&events) //This is obviously not ideal, scales like pure garbage - or does it, now that we clear the outbox?
	if len(events) == 0 {
		return errors.New("No events")
	}

	fmt.Printf("Found events: %v", events)

	newEvents := checkEvents(events) //remove already handled events
	handled := eventHandler(newEvents)

	if !handled { //handler function returns false, sleep 1 sec, retry
		return errors.New("Handling failed")
	}

	s.ClearOutbox(events) //if handler functions returns true, clear the outbox of the handled events
	addToCache(newEvents)
	return nil

}

func (s *Storage) ClearOutbox(events []Event) {
	fmt.Println("Should be clearing out the outbox")
	for _, event := range events {
		s.db.Delete(&event)
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

func addToCache(events []Event) {
	fmt.Println("Adding to cache")
	for _, event := range events {
		if !cacheContains(event.ID) {
			cache[event.ID] = struct{}{}
		}
	}
}
