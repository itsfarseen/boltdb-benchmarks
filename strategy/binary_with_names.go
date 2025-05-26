package strategy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go.etcd.io/bbolt"
)

// 4. Binary with field names strategy
type BinaryWithNamesStrategy struct{}

func (s *BinaryWithNamesStrategy) Name() string { return "Binary+Names" }

func (s *BinaryWithNamesStrategy) Setup(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users_binary_names"))
		return err
	})
}

// Type‐marker constants
const (
	tagInt64   = byte(1)
	tagString  = byte(2)
	tagInt32   = byte(3)
	tagFloat32 = byte(4)
	tagFloat64 = byte(5)
	tagBool    = byte(6)
)

// encodeBinaryWithNames serializes user → []byte, returning any error.
func (s *BinaryWithNamesStrategy) encodeBinaryWithNames(user *UserInfo) ([]byte, error) {
	buf := new(bytes.Buffer)

	// collect fields
	fields := []struct {
		name string
		tag  byte
		data interface{}
	}{
		{"id", tagInt64, user.ID},
		{"username", tagString, user.Username},
		{"email", tagString, user.Email},
		{"first_name", tagString, user.FirstName},
		{"last_name", tagString, user.LastName},
		{"age", tagInt32, user.Age},
		{"height", tagFloat32, user.Height},
		{"weight", tagFloat32, user.Weight},
		{"balance", tagFloat64, user.Balance},
		{"is_active", tagBool, user.IsActive},
		{"created_at", tagInt64, user.CreatedAt},
		{"updated_at", tagInt64, user.UpdatedAt},
		{"login_count", tagInt32, user.LoginCount},
		{"score", tagFloat64, user.Score},
		{"description", tagString, user.Description},
	}

	// write field count
	if err := binary.Write(buf, binary.LittleEndian, int32(len(fields))); err != nil {
		return nil, fmt.Errorf("write field count: %w", err)
	}

	// helper to write each field
	for _, f := range fields {
		// name length + name
		if err := binary.Write(buf, binary.LittleEndian, int32(len(f.name))); err != nil {
			return nil, fmt.Errorf("write name length %s: %w", f.name, err)
		}
		if _, err := buf.WriteString(f.name); err != nil {
			return nil, fmt.Errorf("write name %s: %w", f.name, err)
		}
		// type marker
		if err := buf.WriteByte(f.tag); err != nil {
			return nil, fmt.Errorf("write type tag %s: %w", f.name, err)
		}
		// payload
		switch f.tag {
		case tagInt64:
			if err := binary.Write(buf, binary.LittleEndian, f.data.(int64)); err != nil {
				return nil, fmt.Errorf("write int64 %s: %w", f.name, err)
			}
		case tagString:
			str := f.data.(string)
			if err := binary.Write(buf, binary.LittleEndian, int32(len(str))); err != nil {
				return nil, fmt.Errorf("write str len %s: %w", f.name, err)
			}
			if _, err := buf.WriteString(str); err != nil {
				return nil, fmt.Errorf("write str %s: %w", f.name, err)
			}
		case tagInt32:
			if err := binary.Write(buf, binary.LittleEndian, f.data.(int32)); err != nil {
				return nil, fmt.Errorf("write int32 %s: %w", f.name, err)
			}
		case tagFloat32:
			if err := binary.Write(buf, binary.LittleEndian, f.data.(float32)); err != nil {
				return nil, fmt.Errorf("write float32 %s: %w", f.name, err)
			}
		case tagFloat64:
			if err := binary.Write(buf, binary.LittleEndian, f.data.(float64)); err != nil {
				return nil, fmt.Errorf("write float64 %s: %w", f.name, err)
			}
		case tagBool:
			if err := binary.Write(buf, binary.LittleEndian, f.data.(bool)); err != nil {
				return nil, fmt.Errorf("write bool %s: %w", f.name, err)
			}
		default:
			return nil, fmt.Errorf("unknown tag %d for field %s", f.tag, f.name)
		}
	}

	return buf.Bytes(), nil
}

// decodeBinaryWithNames deserializes []byte → *UserInfo, error on any mismatch.
func (s *BinaryWithNamesStrategy) decodeBinaryWithNames(data []byte) (*UserInfo, error) {
	buf := bytes.NewReader(data)
	user := &UserInfo{}

	var fieldCount int32
	if err := binary.Read(buf, binary.LittleEndian, &fieldCount); err != nil {
		return nil, fmt.Errorf("read field count: %w", err)
	}

	for i := int32(0); i < fieldCount; i++ {
		// read field name
		var nameLen int32
		if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
			return nil, fmt.Errorf("read name length: %w", err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := buf.Read(nameBytes); err != nil {
			return nil, fmt.Errorf("read name: %w", err)
		}
		fieldName := string(nameBytes)

		// read & assert type marker
		tag, err := buf.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read tag %s: %w", fieldName, err)
		}

		// now read payload based on tag
		switch tag {
		case tagInt64:
			var v int64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read int64 %s: %w", fieldName, err)
			}
			switch fieldName {
			case "id":
				user.ID = v
			case "created_at":
				user.CreatedAt = v
			case "updated_at":
				user.UpdatedAt = v
			default:
				return nil, fmt.Errorf("unexpected int64 for %s", fieldName)
			}

		case tagString:
			var strLen int32
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, fmt.Errorf("read str len %s: %w", fieldName, err)
			}
			strBytes := make([]byte, strLen)
			if _, err := buf.Read(strBytes); err != nil {
				return nil, fmt.Errorf("read str %s: %w", fieldName, err)
			}
			switch fieldName {
			case "username":
				user.Username = string(strBytes)
			case "email":
				user.Email = string(strBytes)
			case "first_name":
				user.FirstName = string(strBytes)
			case "last_name":
				user.LastName = string(strBytes)
			case "description":
				user.Description = string(strBytes)
			default:
				return nil, fmt.Errorf("unexpected string for %s", fieldName)
			}

		case tagInt32:
			var v int32
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read int32 %s: %w", fieldName, err)
			}
			switch fieldName {
			case "age":
				user.Age = v
			case "login_count":
				user.LoginCount = v
			default:
				return nil, fmt.Errorf("unexpected int32 for %s", fieldName)
			}

		case tagFloat32:
			var v float32
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read float32 %s: %w", fieldName, err)
			}
			switch fieldName {
			case "height":
				user.Height = v
			case "weight":
				user.Weight = v
			default:
				return nil, fmt.Errorf("unexpected float32 for %s", fieldName)
			}

		case tagFloat64:
			var v float64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read float64 %s: %w", fieldName, err)
			}
			switch fieldName {
			case "balance":
				user.Balance = v
			case "score":
				user.Score = v
			default:
				return nil, fmt.Errorf("unexpected float64 for %s", fieldName)
			}

		case tagBool:
			var v bool
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read bool %s: %w", fieldName, err)
			}
			if fieldName != "is_active" {
				return nil, fmt.Errorf("unexpected bool for %s", fieldName)
			}
			user.IsActive = v

		default:
			return nil, fmt.Errorf("unknown tag %d for field %s", tag, fieldName)
		}
	}

	return user, nil
}

func (s *BinaryWithNamesStrategy) Write(db *bbolt.DB, user *UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary_names"))
		if b == nil {
			return fmt.Errorf("bucket users_binary_names not found")
		}
		data, err := s.encodeBinaryWithNames(user)
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(user.ID))
		return b.Put(key, data)
	})
}

func (s *BinaryWithNamesStrategy) WriteMany(db *bbolt.DB, users []*UserInfo) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary_names"))
		for _, user := range users {
			data, err := s.encodeBinaryWithNames(user)
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

func (s *BinaryWithNamesStrategy) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	var user *UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary_names"))
		if b == nil {
			return fmt.Errorf("bucket users_binary_names not found")
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user %d not found", id)
		}
		var err error
		user, err = s.decodeBinaryWithNames(data)
		return err
	})
	return user, err
}

func (s *BinaryWithNamesStrategy) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	var users []*UserInfo
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary_names"))
		c := b.Cursor()

		startKey := make([]byte, 8)
		binary.BigEndian.PutUint64(startKey, uint64(startId))

		retrieved := 0
		for k, v := c.Seek(startKey); k != nil && retrieved < count; k, v = c.Next() {
			user, err := s.decodeBinaryWithNames(v)
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

func (s *BinaryWithNamesStrategy) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary_names"))
		if b == nil {
			return fmt.Errorf("bucket users_binary_names not found")
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(id))
		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("user %d not found", id)
		}
		user, err := s.decodeBinaryWithNames(data)
		if err != nil {
			return err
		}

		// assign based on fieldName, with full type-checking
		switch fieldName {
		case "username":
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("username must be string, got %T", value)
			}
			user.Username = v
		case "email":
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("email must be string, got %T", value)
			}
			user.Email = v
		case "first_name":
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("first_name must be string, got %T", value)
			}
			user.FirstName = v
		case "last_name":
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("last_name must be string, got %T", value)
			}
			user.LastName = v
		case "description":
			v, ok := value.(string)
			if !ok {
				return fmt.Errorf("description must be string, got %T", value)
			}
			user.Description = v

		case "age":
			v, ok := value.(int32)
			if !ok {
				return fmt.Errorf("age must be int32, got %T", value)
			}
			user.Age = v
		case "height":
			v, ok := value.(float32)
			if !ok {
				return fmt.Errorf("height must be float32, got %T", value)
			}
			user.Height = v
		case "weight":
			v, ok := value.(float32)
			if !ok {
				return fmt.Errorf("weight must be float32, got %T", value)
			}
			user.Weight = v
		case "login_count":
			v, ok := value.(int32)
			if !ok {
				return fmt.Errorf("login_count must be int32, got %T", value)
			}
			user.LoginCount = v

		case "balance":
			v, ok := value.(float64)
			if !ok {
				return fmt.Errorf("balance must be float64, got %T", value)
			}
			user.Balance = v
		case "score":
			v, ok := value.(float64)
			if !ok {
				return fmt.Errorf("score must be float64, got %T", value)
			}
			user.Score = v

		case "is_active":
			v, ok := value.(bool)
			if !ok {
				return fmt.Errorf("is_active must be bool, got %T", value)
			}
			user.IsActive = v

		case "created_at":
			v, ok := value.(int64)
			if !ok {
				return fmt.Errorf("created_at must be int64, got %T", value)
			}
			user.CreatedAt = v
		case "updated_at":
			v, ok := value.(int64)
			if !ok {
				return fmt.Errorf("updated_at must be int64, got %T", value)
			}
			user.UpdatedAt = v

		default:
			return fmt.Errorf("field %q is not updatable", fieldName)
		}

		// re-encode & store
		newData, err := s.encodeBinaryWithNames(user)
		if err != nil {
			return err
		}
		return b.Put(key, newData)
	})
}

func (s *BinaryWithNamesStrategy) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	var sum float64
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("users_binary_names"))
		if b == nil {
			return fmt.Errorf("bucket users_binary_names not found")
		}
		c := b.Cursor()
		processed := 0
		for k, v := c.First(); k != nil && processed < count; k, v = c.Next() {
			user, err := s.decodeBinaryWithNames(v)
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
			default:
				return fmt.Errorf("cannot sum field %q", fieldName)
			}
			processed++
		}
		return nil
	})
	return sum, err
}
