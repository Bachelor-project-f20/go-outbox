package outbox

import (
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
)

type Relay interface {
	Listen(pollWait, sendWait time.Duration, emitter EventEmitter)
	ListenDebug(pollWait, sendWait time.Duration, emitter EventEmitter, debug bool)
}

func NewRelay(db *gorm.DB) (Relay, error) {
	return &Storage{db: db}, nil
}

func (s *Storage) Listen(pollWait, sendWait time.Duration, emitter EventEmitter) {
	s.ListenDebug(pollWait, sendWait, emitter, false)
}

func (s *Storage) ListenDebug(pollWait, sendWait time.Duration, emitter EventEmitter, debug bool) {
	eventChan := make(chan DbEvent, 10)
	//errorsChan := make(chan error)

	go func() {
		for {
			s.findEvent(eventChan, pollWait, sendWait)
			s.sendEvent(eventChan, emitter, debug)
		}
	}()

	fmt.Println("Here 02")
}

func (s *Storage) findEvent(c chan<- DbEvent, pollWait, sendWait time.Duration) {
	// go func() {
	// 	for {
	fmt.Println("FindEvent")
	var events []DbEvent
	testTime := (time.Now().UnixNano() / 1000000) - sendWait.Milliseconds()
	s.db.Where("status = ?", NEW).Or("status = ? AND insert_time < ?", SENDING, testTime).Find(&events)
	s.checkEvents(events, c)
	// 	}
	// }()
}

func (s *Storage) sendEvent(c <-chan DbEvent, emitter EventEmitter, debug bool) {
	// go func() {
	// 	for {
	e, ok := <-c
	if !ok {
		fmt.Println("SendEvent, broken loop. BREAKING")
		//break
	}
	err := emitter.Emit(Event{
		ID:        e.ID,
		EventName: e.EventName,
		Payload:   e.Payload,
		Publisher: e.Publisher,
		Timestamp: e.Timestamp,
	})
	if err != nil {
		fmt.Println("SendEvent, Send Error. CONTINUE")
		//continue
	}
	s.deleteEvent(e)
	// 		if debug {
	// 			return
	// 		}
	// 	}
	// }()
}

func (s *Storage) deleteEvent(event DbEvent) {
	fmt.Println("Deleting event: ", event.ID)
	s.db.Delete(&event)
}

func (s *Storage) checkEvents(events []DbEvent, c chan<- DbEvent) {
	for _, event := range events {
		if event.Status == NEW {
			fmt.Println("NEW Start sending event: ", event.ID)
			event.Status = SENDING
			s.db.Model(&event).Update(DbEvent{InsertTime: event.InsertTime, Status: event.Status})
			c <- event
		} else if event.Status == SENDING {
			fmt.Println("RE-SENDING event: ", event.ID)
			event.InsertTime = time.Now().UnixNano() / 1000000
			s.db.Model(&event).Update(DbEvent{InsertTime: event.InsertTime})
			c <- event
		}
	}
}
