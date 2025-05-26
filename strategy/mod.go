package strategy

import (
	"go.etcd.io/bbolt"
)

// Storage strategy interface
type StorageStrategy interface {
	Name() string
	Write(db *bbolt.DB, user *UserInfo) error
	WriteMany(db *bbolt.DB, users []*UserInfo) error
	Read(db *bbolt.DB, id int64) (*UserInfo, error)
	ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error)
	UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error
	ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error)
	Setup(db *bbolt.DB) error
}
