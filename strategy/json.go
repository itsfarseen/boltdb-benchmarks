package strategy

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go.etcd.io/bbolt"
)

// 1. JSON encoding strategy
type JSONStrategy struct{}

func (s *JSONStrategy) Name() string { return "JSON" }

func (s *JSONStrategy) Setup(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users_json"))
		return err
	})
}

func (s *JSONStrategy) Write(db *bbolt.DB, user *UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_json"))
		data, err := json.Marshal(user)
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(user.ID))
		return b.Put(key, data)
	})
}

func (s *JSONStrategy) WriteMany(db *bbolt.DB, users []*UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_json"))
		for _, user := range users {
			data, err := json.Marshal(user)
			if err != nil {
				return err
			}
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(user.ID))
			if err := b.Put(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *JSONStrategy) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	var user UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_json"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user not found")
		}
		return json.Unmarshal(data, &user)
	})
	return &user, err
}

func (s *JSONStrategy) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	var users []*UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_json"))
		c := b.Cursor()

		// Seek to start position
		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(startId))

		retrieved := 0
		for k, v := c.Seek(startKey); k != nil && retrieved < count; k, v = c.Next() {
			var user UserInfo
			if err := json.Unmarshal(v, &user); err != nil {
				return err
			}
			users = append(users, &user)
			retrieved++
		}
		return nil
	})
	return users, err
}

func (s *JSONStrategy) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_json"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user not found")
		}

		var user UserInfo
		if err := json.Unmarshal(data, &user); err != nil {
			return err
		}

		// Update the field based on field name
		switch fieldName {
		case "balance":
			user.Balance = value.(float64)
		case "login_count":
			user.LoginCount = value.(int32)
		case "score":
			user.Score = value.(float64)
		}

		newData, err := json.Marshal(&user)
		if err != nil {
			return err
		}
		return b.Put(key, newData)
	})
}

func (s *JSONStrategy) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	var sum float64
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_json"))
		c := b.Cursor()
		processed := 0

		for k, v := c.First(); k != nil && processed < count; k, v = c.Next() {
			var user UserInfo
			if err := json.Unmarshal(v, &user); err != nil {
				return err
			}

			switch fieldName {
			case "balance":
				sum += user.Balance
			case "score":
				sum += user.Score
			case "login_count":
				sum += float64(user.LoginCount)
			}
			processed++
		}
		return nil
	})
	return sum, err
}
