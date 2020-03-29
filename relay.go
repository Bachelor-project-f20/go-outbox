package outbox

import (
	"fmt"
	"log"
	"time"

	etg "github.com/Bachelor-project-f20/eventToGo"
	"github.com/jinzhu/gorm"
)

type relay struct {
	db        *gorm.DB
	eventChan chan DbEvent
}

func NewRelay(db *gorm.DB, pollWait time.Duration, eventChan chan DbEvent, emitter etg.EventEmitter) error {
	r := relay{
		db,
		eventChan,
	}
	r.findEvent(eventChan, pollWait)
	r.sendEvent(eventChan, emitter)
	return nil
}

func (s *relay) findEvent(c chan<- DbEvent, pollWait time.Duration) {
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			var events []DbEvent
			tx := s.db.Begin()
			testTime := (time.Now().UnixNano() / 1000000) - pollWait.Milliseconds()
			tx.Not("service_id = ?", serviceID).Where("insert_time < ?", testTime).Find(&events)
			s.checkEvents(events, c, tx)
			tx.Commit()
		}
	}()
}

func (s *relay) sendEvent(c <-chan DbEvent, emitter etg.EventEmitter) {
	go func() {
		for {
			e, ok := <-c
			if !ok {
				log.Println("SendEvent, broken loop. BREAKING")
			}
			fmt.Println("Sending event: ", e.ID)
			err := emitter.Emit(etg.Event{
				ID:        e.ID,
				EventName: e.EventName,
				Payload:   e.Payload,
				Publisher: e.Publisher,
				TimeStamp: e.Timestamp,
			})
			if err != nil {
				log.Println("SendEvent, Send Error. CONTINUE")
			}
			s.deleteEvent(e)
		}
	}()
}

func (s *relay) deleteEvent(event DbEvent) {
	s.db.Delete(&event)
}

func (s *relay) checkEvents(events []DbEvent, c chan<- DbEvent, tx *gorm.DB) {
	for _, event := range events {
		event.InsertTime = time.Now().UnixNano() / 1000000
		tx.Model(&event).Update(DbEvent{InsertTime: event.InsertTime, ServiceID: serviceID})
		c <- event
	}
}
