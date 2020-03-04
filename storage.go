package outbox

import "github.com/jinzhu/gorm"

type Storage struct {
	db *gorm.DB
}
