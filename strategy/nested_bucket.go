package strategy

import (
	"encoding/binary"
	"fmt"
	"go.etcd.io/bbolt"
	"strconv"
)

// 6. Nested bucket strategy
type NestedBucketStrategy struct{}

func (s *NestedBucketStrategy) Name() string { return "NestedBucket" }

func (s *NestedBucketStrategy) Setup(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users_nested"))
		return err
	})
}

func (s *NestedBucketStrategy) writeUserFields(rootBucket *bbolt.Bucket, user *UserInfo) error {
	userKey := make([]byte, 8)
	binary.BigEndian.PutUint64(userKey, uint64(user.ID))

	userBucket, err := rootBucket.CreateBucketIfNotExists(userKey)
	if err != nil {
		return err
	}

	// Store each field in the user's bucket
	userBucket.Put([]byte("id"), []byte(strconv.FormatInt(user.ID, 10)))
	userBucket.Put([]byte("username"), []byte(user.Username))
	userBucket.Put([]byte("email"), []byte(user.Email))
	userBucket.Put([]byte("first_name"), []byte(user.FirstName))
	userBucket.Put([]byte("last_name"), []byte(user.LastName))
	userBucket.Put([]byte("age"), []byte(strconv.FormatInt(int64(user.Age), 10)))

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, uint32(user.Height))
	userBucket.Put([]byte("height"), heightBytes)

	weightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(weightBytes, uint32(user.Weight))
	userBucket.Put([]byte("weight"), weightBytes)

	balanceBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(balanceBytes, uint64(user.Balance))
	userBucket.Put([]byte("balance"), balanceBytes)

	activeBytes := []byte("false")
	if user.IsActive {
		activeBytes = []byte("true")
	}
	userBucket.Put([]byte("is_active"), activeBytes)

	userBucket.Put([]byte("created_at"), []byte(strconv.FormatInt(user.CreatedAt, 10)))
	userBucket.Put([]byte("updated_at"), []byte(strconv.FormatInt(user.UpdatedAt, 10)))
	userBucket.Put([]byte("login_count"), []byte(strconv.FormatInt(int64(user.LoginCount), 10)))

	scoreBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(scoreBytes, uint64(user.Score))
	userBucket.Put([]byte("score"), scoreBytes)

	userBucket.Put([]byte("description"), []byte(user.Description))

	return nil
}

func (s *NestedBucketStrategy) Write(db *bbolt.DB, user *UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket([]byte("users_nested"))
		return s.writeUserFields(rootBucket, user)
	})
}

func (s *NestedBucketStrategy) WriteMany(db *bbolt.DB, users []*UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket([]byte("users_nested"))
		for _, user := range users {
			if err := s.writeUserFields(rootBucket, user); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *NestedBucketStrategy) decodeField(user *UserInfo, field string, data []byte) {
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

func (s *NestedBucketStrategy) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	user := &UserInfo{}
	err := db.View(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte("users_nested"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))

		userBucket := root.Bucket(key)
		if userBucket == nil {
			return fmt.Errorf("user not found")
		}

		c := userBucket.Cursor()
		for fk, fv := c.First(); fk != nil; fk, fv = c.Next() {
			s.decodeField(user, string(fk), fv)
		}
		return nil
	})
	return user, err
}

func (s *NestedBucketStrategy) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	var users []*UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte("users_nested"))
		c := root.Cursor()

		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(startId))

		for uk, _ := c.Seek(startKey); uk != nil && len(users) < count; uk, _ = c.Next() {
			// id := int64(binary.BigEndian.Uint64(uk))
			userBucket := root.Bucket(uk)
			if userBucket == nil {
				continue
			}

			user := &UserInfo{}
			uc := userBucket.Cursor()
			for fk, fv := uc.First(); fk != nil; fk, fv = uc.Next() {
				s.decodeField(user, string(fk), fv)
			}
			users = append(users, user)
		}
		return nil
	})
	return users, err
}

func (s *NestedBucketStrategy) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket([]byte("users_nested"))
		userKey := make([]byte, 8)
		binary.BigEndian.PutUint64(userKey, uint64(id))

		userBucket := rootBucket.Bucket(userKey)
		if userBucket == nil {
			return fmt.Errorf("user not found")
		}

		switch fieldName {
		case "balance":
			balanceBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(balanceBytes, uint64(value.(float64)))
			return userBucket.Put([]byte("balance"), balanceBytes)
		case "login_count":
			return userBucket.Put([]byte("login_count"), []byte(strconv.FormatInt(int64(value.(int32)), 10)))
		case "score":
			scoreBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(scoreBytes, uint64(value.(float64)))
			return userBucket.Put([]byte("score"), scoreBytes)
		}
		return nil
	})
}

func (s *NestedBucketStrategy) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	var sum float64
	err := db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket([]byte("users_nested"))
		c := rootBucket.Cursor()
		processed := 0

		for k, _ := c.First(); k != nil && processed < count; k, _ = c.Next() {
			userBucket := rootBucket.Bucket(k)
			if userBucket != nil {
				if data := userBucket.Get([]byte(fieldName)); data != nil {
					switch fieldName {
					case "balance", "score":
						sum += float64(binary.LittleEndian.Uint64(data))
					case "login_count":
						count, _ := strconv.ParseInt(string(data), 10, 32)
						sum += float64(count)
					}
				}
				processed++
			}
		}
		return nil
	})
	return sum, err
}
