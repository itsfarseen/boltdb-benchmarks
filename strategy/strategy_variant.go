package strategy

import (
	"go.etcd.io/bbolt"
)

type StrategyVariant struct {
	Strategy StorageStrategy
	Bulk     bool
}

func (sv *StrategyVariant) Name() string {
	return sv.Strategy.Name()
}

func (sv *StrategyVariant) Setup(db *bbolt.DB) error { return sv.Strategy.Setup(db) }
func (sv *StrategyVariant) Read(db *bbolt.DB, id int64) (*UserInfo, error) {
	return sv.Strategy.Read(db, id)
}
func (sv *StrategyVariant) ReadMany(db *bbolt.DB, startId int64, count int) ([]*UserInfo, error) {
	return sv.Strategy.ReadMany(db, startId, count)
}
func (sv *StrategyVariant) UpdateField(db *bbolt.DB, id int64, fieldName string, value interface{}) error {
	return sv.Strategy.UpdateField(db, id, fieldName, value)
}
func (sv *StrategyVariant) ReadFieldSum(db *bbolt.DB, fieldName string, count int) (float64, error) {
	return sv.Strategy.ReadFieldSum(db, fieldName, count)
}

func (sv *StrategyVariant) WriteAll(db *bbolt.DB, users []*UserInfo) error {
	if sv.Bulk {
		return sv.Strategy.WriteMany(db, users)
	} else {
		for _, user := range users {
			if err := sv.Strategy.Write(db, user); err != nil {
				return err
			}
		}
		return nil
	}
}

// WriteMode determines how writes are performed during benchmarks
func (sv *StrategyVariant) WriteMode() bool {
	return sv.Bulk
}
