package strategy

import (
	"bytes"
	"encoding/binary"
	"go.etcd.io/bbolt"
	"strconv"
)

// 5. Multiple KV pairs strategy
type MultiKVStrategy struct{}

func (s *MultiKVStrategy) Name() string { return "MultiKV" }

func (s *MultiKVStrategy) Setup(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users_multikv"))
		return err
	})
}

func (s *MultiKVStrategy) makeKey(id int64, field string) []byte {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, uint64(id))
	return append(idBytes, []byte(field)...)
}

func (s *MultiKVStrategy) decodeField(user *UserInfo, field string, data []byte) {
	switch field {
	case "id":
		user.ID, _ = strconv.ParseInt(string(data), 10, 64)
	case "username":
		user.Username = string(data)
	case "email":
		user.Email = string(data)
	case "first_name":
		user.FirstName = string(data)
	case "last_name":
		user.LastName = string(data)
	case "age":
		age, _ := strconv.ParseInt(string(data), 10, 32)
		user.Age = int32(age)
	case "height":
		user.Height = float32(binary.LittleEndian.Uint32(data))
	case "weight":
		user.Weight = float32(binary.LittleEndian.Uint32(data))
	case "balance":
		user.Balance = float64(binary.LittleEndian.Uint64(data))
	case "is_active":
		user.IsActive = string(data) == "true"
	case "created_at":
		user.CreatedAt, _ = strconv.ParseInt(string(data), 10, 64)
	case "updated_at":
		user.UpdatedAt, _ = strconv.ParseInt(string(data), 10, 64)
	case "login_count":
		cnt, _ := strconv.ParseInt(string(data), 10, 32)
		user.LoginCount = int32(cnt)
	case "score":
		user.Score = float64(binary.LittleEndian.Uint64(data))
	case "description":
		user.Description = string(data)
	}
}

func (s *MultiKVStrategy) writeUserFields(b *bbolt.Bucket, user *UserInfo) error {
	// Store each field as a separate KV pair
	if err := b.Put(s.makeKey(user.ID, "id"), []byte(strconv.FormatInt(user.ID, 10))); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "username"), []byte(user.Username)); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "email"), []byte(user.Email)); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "first_name"), []byte(user.FirstName)); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "last_name"), []byte(user.LastName)); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "age"), []byte(strconv.FormatInt(int64(user.Age), 10))); err != nil {
		return err
	}

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, uint32(user.Height))
	if err := b.Put(s.makeKey(user.ID, "height"), heightBytes); err != nil {
		return err
	}

	weightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(weightBytes, uint32(user.Weight))
	if err := b.Put(s.makeKey(user.ID, "weight"), weightBytes); err != nil {
		return err
	}

	balanceBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(balanceBytes, uint64(user.Balance))
	if err := b.Put(s.makeKey(user.ID, "balance"), balanceBytes); err != nil {
		return err
	}

	activeBytes := []byte("false")
	if user.IsActive {
		activeBytes = []byte("true")
	}
	if err := b.Put(s.makeKey(user.ID, "is_active"), activeBytes); err != nil {
		return err
	}

	if err := b.Put(s.makeKey(user.ID, "created_at"), []byte(strconv.FormatInt(user.CreatedAt, 10))); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "updated_at"), []byte(strconv.FormatInt(user.UpdatedAt, 10))); err != nil {
		return err
	}
	if err := b.Put(s.makeKey(user.ID, "login_count"), []byte(strconv.FormatInt(int64(user.LoginCount), 10))); err != nil {
		return err
	}

	scoreBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(scoreBytes, uint64(user.Score))
	if err := b.Put(s.makeKey(user.ID, "score"), scoreBytes); err != nil {
		return err
	}

	if err := b.Put(s.makeKey(user.ID, "description"), []byte(user.Description)); err != nil {
		return err
	}

	return nil
}

func (s *MultiKVStrategy) Write(db *bbolt.DB, user *UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_multikv"))
		return s.writeUserFields(b, user)
	})
}

func (s *MultiKVStrategy) WriteMany(db *bbolt.DB, users []*UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_multikv"))
		for _, user := range users {
			if err := s.writeUserFields(b, user); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *MultiKVStrategy) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	user := &UserInfo{}
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_multikv"))
		c := b.Cursor()

		// seek to the first key for this id
		prefix := make([]byte, 8)
		binary.BigEndian.PutUint64(prefix, uint64(id))
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			field := string(k[8:])
			s.decodeField(user, field, v)
		}
		return nil
	})
	return user, err
}

func (s *MultiKVStrategy) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	var users []*UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_multikv"))
		c := b.Cursor()

		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(startId))

		var currentId int64
		var currentUser *UserInfo

		for k, v := c.Seek(startKey); k != nil && len(users) < count; k, v = c.Next() {
			if len(k) < 8 {
				continue
			}
			id := int64(binary.BigEndian.Uint64(k[:8]))
			if id < startId {
				continue
			}
			if id != currentId {
				if currentUser != nil {
					users = append(users, currentUser)
					if len(users) >= count {
						break
					}
				}
				currentId = id
				currentUser = &UserInfo{}
			}
			field := string(k[8:])
			s.decodeField(currentUser, field, v)
		}
		if currentUser != nil && len(users) < count {
			users = append(users, currentUser)
		}
		return nil
	})
	return users, err
}

func (s *MultiKVStrategy) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_multikv"))

		switch fieldName {
		case "balance":
			balanceBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(balanceBytes, uint64(value.(float64)))
			return b.Put(s.makeKey(id, "balance"), balanceBytes)
		case "login_count":
			return b.Put(s.makeKey(id, "login_count"), []byte(strconv.FormatInt(int64(value.(int32)), 10)))
		case "score":
			scoreBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(scoreBytes, uint64(value.(float64)))
			return b.Put(s.makeKey(id, "score"), scoreBytes)
		}
		return nil
	})
}

func (s *MultiKVStrategy) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	var sum float64
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_multikv"))
		c := b.Cursor()

		// Look for keys with the specific field suffix
		fieldSuffix := []byte(fieldName)
		processed := 0
		seenIDs := make(map[int64]bool)

		for k, v := c.First(); k != nil && processed < count; k, v = c.Next() {
			if len(k) >= len(fieldSuffix) && bytes.Equal(k[len(k)-len(fieldSuffix):], fieldSuffix) {
				// Extract ID from key
				idBytes := k[:8]
				id := int64(binary.BigEndian.Uint64(idBytes))

				if !seenIDs[id] {
					seenIDs[id] = true
					processed++

					switch fieldName {
					case "balance", "score":
						sum += float64(binary.LittleEndian.Uint64(v))
					case "login_count":
						count, _ := strconv.ParseInt(string(v), 10, 32)
						sum += float64(count)
					}
				}
			}
		}
		return nil
	})
	return sum, err
}
