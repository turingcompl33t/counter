package counter

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Counter struct {
	gorm.Model
	Name  string `gorm:"not null;unique"`
	Value uint32 `gorm:"not null"`
}

type CounterOperator struct{}

// Create a new named counter.
func (co CounterOperator) CreateCounter(ctx context.Context, tx *gorm.DB, name string) (Counter, error) {
	counter := Counter{Name: name, Value: 0}
	if err := tx.Create(&counter).Error; err != nil {
		return Counter{}, err
	}
	return counter, nil
}

// Get the next value of the named counter.
func (co CounterOperator) Next(ctx context.Context, tx *gorm.DB, name string) (Counter, error) {
	var counter Counter
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("name = ?", name).First(&counter).Error; err != nil {
		return Counter{}, err
	}

	counter.Value++
	if err := tx.Save(&counter).Error; err != nil {
		return Counter{}, err
	}

	return counter, nil
}

// Delete the named counter.
func (co CounterOperator) DeleteCounter(ctx context.Context, tx *gorm.DB, name string) error {
	return tx.Where("name = ?", name).Delete(&Counter{}).Error
}
