package strategy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go.etcd.io/bbolt"
)

// 3. Binary encoding strategy (values only)
type BinaryStrategy struct{}

func (s *BinaryStrategy) Name() string { return "Binary" }

func (s *BinaryStrategy) Setup(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users_binary"))
		return err
	})
}

func (s *BinaryStrategy) encodeBinary(user *UserInfo) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, user.ID)

	// String fields with length prefix
	binary.Write(buf, binary.LittleEndian, int32(len(user.Username)))
	buf.WriteString(user.Username)
	binary.Write(buf, binary.LittleEndian, int32(len(user.Email)))
	buf.WriteString(user.Email)
	binary.Write(buf, binary.LittleEndian, int32(len(user.FirstName)))
	buf.WriteString(user.FirstName)
	binary.Write(buf, binary.LittleEndian, int32(len(user.LastName)))
	buf.WriteString(user.LastName)
	binary.Write(buf, binary.LittleEndian, int32(len(user.Description)))
	buf.WriteString(user.Description)

	// Fixed-size fields
	binary.Write(buf, binary.LittleEndian, user.Age)
	binary.Write(buf, binary.LittleEndian, user.Height)
	binary.Write(buf, binary.LittleEndian, user.Weight)
	binary.Write(buf, binary.LittleEndian, user.Balance)
	binary.Write(buf, binary.LittleEndian, user.IsActive)
	binary.Write(buf, binary.LittleEndian, user.CreatedAt)
	binary.Write(buf, binary.LittleEndian, user.UpdatedAt)
	binary.Write(buf, binary.LittleEndian, user.LoginCount)
	binary.Write(buf, binary.LittleEndian, user.Score)

	return buf.Bytes()
}

func (s *BinaryStrategy) decodeBinary(data []byte) (*UserInfo, error) {
	buf := bytes.NewReader(data)
	user := &UserInfo{}

	binary.Read(buf, binary.LittleEndian, &user.ID)

	// Read string fields
	var strLen int32

	binary.Read(buf, binary.LittleEndian, &strLen)
	usernameBytes := make([]byte, strLen)
	buf.Read(usernameBytes)
	user.Username = string(usernameBytes)

	binary.Read(buf, binary.LittleEndian, &strLen)
	emailBytes := make([]byte, strLen)
	buf.Read(emailBytes)
	user.Email = string(emailBytes)

	binary.Read(buf, binary.LittleEndian, &strLen)
	firstNameBytes := make([]byte, strLen)
	buf.Read(firstNameBytes)
	user.FirstName = string(firstNameBytes)

	binary.Read(buf, binary.LittleEndian, &strLen)
	lastNameBytes := make([]byte, strLen)
	buf.Read(lastNameBytes)
	user.LastName = string(lastNameBytes)

	binary.Read(buf, binary.LittleEndian, &strLen)
	descBytes := make([]byte, strLen)
	buf.Read(descBytes)
	user.Description = string(descBytes)

	// Read fixed-size fields
	binary.Read(buf, binary.LittleEndian, &user.Age)
	binary.Read(buf, binary.LittleEndian, &user.Height)
	binary.Read(buf, binary.LittleEndian, &user.Weight)
	binary.Read(buf, binary.LittleEndian, &user.Balance)
	binary.Read(buf, binary.LittleEndian, &user.IsActive)
	binary.Read(buf, binary.LittleEndian, &user.CreatedAt)
	binary.Read(buf, binary.LittleEndian, &user.UpdatedAt)
	binary.Read(buf, binary.LittleEndian, &user.LoginCount)
	binary.Read(buf, binary.LittleEndian, &user.Score)

	return user, nil
}

func (s *BinaryStrategy) Write(db *bbolt.DB, user *UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary"))
		data := s.encodeBinary(user)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(user.ID))
		return b.Put(key, data)
	})
}

func (s *BinaryStrategy) WriteMany(db *bbolt.DB, users []*UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary"))
		for _, user := range users {
			data := s.encodeBinary(user)
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(user.ID))
			if err := b.Put(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BinaryStrategy) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	var user *UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user not found")
		}
		var err error
		user, err = s.decodeBinary(data)
		return err
	})
	return user, err
}

func (s *BinaryStrategy) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	var users []*UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary"))
		c := b.Cursor()

		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(startId))

		retrieved := 0
		for k, v := c.Seek(startKey); k != nil && retrieved < count; k, v = c.Next() {
			user, err := s.decodeBinary(v)
			if err != nil {
				return err
			}
			users = append(users, user)
			retrieved++
		}
		return nil
	})
	return users, err
}

func (s *BinaryStrategy) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user not found")
		}

		user, err := s.decodeBinary(data)
		if err != nil {
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

		newData := s.encodeBinary(user)
		return b.Put(key, newData)
	})
}

func (s *BinaryStrategy) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	var sum float64
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary"))
		c := b.Cursor()
		processed := 0

		for k, v := c.First(); k != nil && processed < count; k, v = c.Next() {
			user, err := s.decodeBinary(v)
			if err != nil {
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
