package strategy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"go.etcd.io/bbolt"
)

// 2. GOB encoding strategy
type GOBStrategy struct{}

func (s *GOBStrategy) Name() string { return "GOB" }

func (s *GOBStrategy) Setup(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users_gob"))
		return err
	})
}

func (s *GOBStrategy) Write(db *bbolt.DB, user *UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_gob"))
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(user); err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(user.ID))
		return b.Put(key, buf.Bytes())
	})
}

func (s *GOBStrategy) WriteMany(db *bbolt.DB, users []*UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_gob"))
		for _, user := range users {
			var buf bytes.Buffer
			encoder := gob.NewEncoder(&buf)
			if err := encoder.Encode(user); err != nil {
				return err
			}
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(user.ID))
			if err := b.Put(key, buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *GOBStrategy) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	var user UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_gob"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user not found")
		}
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		return decoder.Decode(&user)
	})
	return &user, err
}

func (s *GOBStrategy) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	var users []*UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_gob"))
		c := b.Cursor()

		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(startId))

		retrieved := 0
		for k, v := c.Seek(startKey); k != nil && retrieved < count; k, v = c.Next() {
			var user UserInfo
			buf := bytes.NewBuffer(v)
			decoder := gob.NewDecoder(buf)
			if err := decoder.Decode(&user); err != nil {
				return err
			}
			users = append(users, &user)
			retrieved++
		}
		return nil
	})
	return users, err
}

func (s *GOBStrategy) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_gob"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user not found")
		}

		var user UserInfo
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		if err := decoder.Decode(&user); err != nil {
			return err
		}

		switch fieldName {
		case "balance":
			user.Balance = value.(float64)
		case "login_count":
			user.LoginCount = value.(int32)
		case "score":
			user.Score = value.(float64)
		}

		var newBuf bytes.Buffer
		encoder := gob.NewEncoder(&newBuf)
		if err := encoder.Encode(&user); err != nil {
			return err
		}
		return b.Put(key, newBuf.Bytes())
	})
}

func (s *GOBStrategy) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	var sum float64
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_gob"))
		c := b.Cursor()
		processed := 0

		for k, v := c.First(); k != nil && processed < count; k, v = c.Next() {
			var user UserInfo
			buf := bytes.NewBuffer(v)
			decoder := gob.NewDecoder(buf)
			if err := decoder.Decode(&user); err != nil {
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
