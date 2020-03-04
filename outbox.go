package outbox

import (
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type DbType int

const (
	MySQL DbType = iota
	Postgres
)

type Outbox interface {
	Insert(obj interface{}, e Event) error
	Update(obj interface{}, e Event) error
	Delete(obj interface{}, e Event) error
	GetDBConnection() *gorm.DB
	Close()
}

type schemaType struct {
	schema interface{}
}

type Storage struct {
	db *gorm.DB
}

type Event struct {
	ID        string
	Publisher string
	EventName string
	Timestamp int64
	Payload   []byte
}

func NewOutbox(dbType DbType, dbString string, schemas ...interface{}) (Outbox, error) {
	db := connect(dbType, dbString)

	schemaTypes := make([]interface{}, 0)
	for _, schema := range schemas {
		schemaTypes = append(schemaTypes, schema)
	}

	schemaTypes = append(schemaTypes, Event{})

	err := createSchema(db, schemaTypes)
	if err != nil {
		return nil, err
	}
	return &Storage{db: db}, nil
}

func (s *Storage) Insert(obj interface{}, e Event) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(obj).Error; err != nil {
			return err
		}
		if err := tx.Create(e).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *Storage) Update(obj interface{}, e Event) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(obj).Error; err != nil {
			return err
		}
		if err := tx.Create(e).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *Storage) Delete(obj interface{}, e Event) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(obj).Error; err != nil {
			return err
		}
		if err := tx.Create(e).Error; err != nil {
			return err
		}
		return nil
	})
}

func createSchema(db *gorm.DB, schemaModels []interface{}) error {
	db.AutoMigrate(schemaModels...)
	return nil
}

func (s *Storage) Close() {
	s.db.Close()
}

func connect(dbType DbType, dbString string) *gorm.DB {
	i := 5
	for i > 0 {
		db, err := gorm.Open(getType(dbType), dbString)
		if err != nil {
			fmt.Println("Can't connect to db, sleeping for 2 sec, err: ", err)
			time.Sleep(2 * time.Second)
			i--
			continue
		} else {
			fmt.Println("Connected to storage")
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

func (s *Storage) GetDBConnection() *gorm.DB {
	return s.db
}
