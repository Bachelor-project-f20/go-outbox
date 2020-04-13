package outbox

import (
	"log"
	"time"

	etg "github.com/Bachelor-project-f20/eventToGo"
	models "github.com/Bachelor-project-f20/shared/models"
	"github.com/gofrs/uuid"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var serviceID string

type DbType int

const (
	MySQL DbType = iota
	Postgres
)

type schemaType struct {
	schema interface{}
}

type DbEvent struct {
	ID         string
	Publisher  string
	EventName  string
	Timestamp  int64
	Payload    []byte
	InsertTime int64
	ServiceID  string
	ApiTag     string
}

type Outbox interface {
	Insert(obj interface{}, e models.Event) error
	Update(obj interface{}, e models.Event) error
	Delete(obj interface{}, e models.Event) error
	GetDBConnection() *gorm.DB
	Close()
}

type outbox struct {
	db        *gorm.DB
	eventChan chan DbEvent
}

func NewOutbox(dbType DbType, dbString string, connectionSleepInSec int, emitter etg.EventEmitter, schemas ...interface{}) (Outbox, error) {
	newID, _ := uuid.NewV4()
	serviceID = newID.String()

	db := connect(dbType, dbString, connectionSleepInSec)

	schemaTypes := make([]interface{}, 0)
	for _, schema := range schemas {
		schemaTypes = append(schemaTypes, schema)
	}

	schemaTypes = append(schemaTypes, DbEvent{})

	err := createSchema(db, schemaTypes)
	if err != nil {
		return nil, err
	}

	eventChan := make(chan DbEvent, 10)
	err = NewRelay(db, 30, eventChan, emitter)
	if err != nil {
		return nil, err
	}

	return &outbox{
		db,
		eventChan,
	}, nil
}

func (s *outbox) Insert(obj interface{}, e models.Event) error {
	dbEvent := createDbEvent(e)
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(obj).Error; err != nil {
			return err
		}
		if err := tx.Create(dbEvent).Error; err != nil {
			return err
		}
		s.eventChan <- dbEvent
		return nil
	})
}

func (s *outbox) Update(obj interface{}, e models.Event) error {
	dbEvent := createDbEvent(e)
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(obj).Error; err != nil {
			return err
		}
		if err := tx.Create(dbEvent).Error; err != nil {
			return err
		}
		s.eventChan <- dbEvent
		return nil
	})
}

func (s *outbox) Delete(obj interface{}, e models.Event) error {
	dbEvent := createDbEvent(e)
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(obj).Error; err != nil {
			return err
		}
		if err := tx.Create(dbEvent).Error; err != nil {
			return err
		}
		s.eventChan <- dbEvent
		return nil
	})
}

func createDbEvent(e models.Event) DbEvent {
	return DbEvent{
		e.ID,
		e.Publisher,
		e.EventName,
		e.Timestamp,
		e.Payload,
		time.Now().UnixNano() / 1000000, //to millis
		serviceID,
		e.ApiTag}
}

func createSchema(db *gorm.DB, schemaModels []interface{}) error {
	db.AutoMigrate(schemaModels...)
	return nil
}

func (s *outbox) Close() {
	s.db.Close()
}

func connect(dbType DbType, dbString string, connectionSleepInSec int) *gorm.DB {
	i := 5
	for i > 0 {
		db, err := gorm.Open(getType(dbType), dbString)
		if err != nil {
			log.Println("Can't connect to db, sleeping for 2 sec, err: ", err)
			time.Sleep(time.Duration(connectionSleepInSec) * time.Second)
			i--
			continue
		} else {
			log.Println("Connected to storage")
			return db
		}
	}
	panic("Not connected to storage")
}

func getType(dbType DbType) string {
	switch dbType {
	case MySQL:
		return "mysql"
	case Postgres:
		return "postgres"
	}
	panic("Database type not supported")
}

func (s *outbox) GetDBConnection() *gorm.DB {
	return s.db
}
