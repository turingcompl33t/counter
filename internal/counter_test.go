package counter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Parameterize the test suite over backing DBMS implementations.
var parameters = []struct {
	name   string
	initFn func(*testing.T) (*gorm.DB, func())
}{
	{"sqlite", newSqliteDB},
	{"postgres", newPostgresDB},
}

// A counter can be initialized.
func TestInit(t *testing.T) {
	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}
		})

	}
}

// Calling 'next' without first creating a counter fails.
func TestNextWithoutCreate(t *testing.T) {
	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.Next(context.Background(), db, "n0")
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// The counter works in the most basic case - with a single name.
func TestSingleName(t *testing.T) {
	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < 10; i++ {
				c, err := co.Next(context.Background(), db, "n0")
				if err != nil {
					t.Fatal(err)
				}
				if c.Value != uint32(i+1) {
					t.Fatalf("expected %d, got %d", i+1, c.Value)
				}
			}
		})
	}
}

// Multiple counters can be run concurrently.
func TestMultipleNames(t *testing.T) {
	const nValues = 10

	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			_, err = co.CreateCounter(context.Background(), db, "n1")
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < nValues; i++ {
				x, err := co.Next(context.Background(), db, "n0")
				if err != nil {
					t.Fatal(err)
				}
				if x.Value != uint32(i+1) {
					t.Fatalf("expected %d, got %d", i+1, x.Value)
				}

				y, err := co.Next(context.Background(), db, "n1")
				if err != nil {
					t.Fatal(err)
				}
				if y.Value != uint32(i+1) {
					t.Fatalf("expected %d, got %d", i+1, y.Value)
				}
			}
		})

	}
}

// Test row-level locking under minimal contention.
func TestContention(t *testing.T) {
	const nThreads = 2

	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			// Results
			rs := make(chan uint32, 2)

			// Error channel
			ec := make(chan error, 2)

			var wg sync.WaitGroup
			wg.Add(2)

			for i := 0; i < nThreads; i++ {
				go func() {
					defer wg.Done()

					db.Transaction(func(tx *gorm.DB) error {
						c, err := co.Next(context.Background(), tx, "n0")
						if err != nil {
							ec <- err
							return err
						}

						rs <- c.Value
						return nil
					})
				}()
			}

			wg.Wait()

			if len(ec) > 0 {
				t.Fatal(<-ec)
			}

			if r0, r1 := <-rs, <-rs; r0+r1 != 3 {
				t.Fatalf("expected sum of 3, got %d", r0+r1)
			}
		})
	}
}

// Counters behave correctly in the presence of concurrent transactions.
func TestSingleNameConcurrent(t *testing.T) {
	const nThreads = 10

	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			// Record values
			values := make(map[uint32]bool)

			var lk sync.Mutex
			var wg sync.WaitGroup

			ec := make(chan error, nThreads)

			wg.Add(nThreads)
			for i := 0; i < nThreads; i++ {
				go func() {
					defer wg.Done()

					db.Transaction(func(tx *gorm.DB) error {
						c, err := co.Next(context.Background(), tx, "n0")
						if err != nil {
							ec <- err
							return err
						}

						lk.Lock()
						defer lk.Unlock()

						values[c.Value] = true
						return nil
					})
				}()
			}

			wg.Wait()

			if len(ec) > 0 {
				t.Fatal(<-ec)
			}

			if len(values) != nThreads {
				t.Fatalf("expected %d, got %d", nThreads, len(values))
			}

			for i := 1; i <= nThreads; i++ {
				if !values[uint32(i)] {
					t.Fatalf("missing %d", i)
				}
			}
		})
	}
}

// Multiple counters behave correctly in the presence of concurrent transactions.
func TestMultipleNamesConcurrent(t *testing.T) {
	const nThreads = 10

	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			_, err = co.CreateCounter(context.Background(), db, "n1")
			if err != nil {
				t.Fatal(err)
			}

			// Record values
			values := make(map[string]map[uint32]bool)

			var lk sync.Mutex
			var wg sync.WaitGroup

			ec := make(chan error, nThreads)

			wg.Add(nThreads)
			for i := 0; i < nThreads; i++ {
				go func() {
					defer wg.Done()

					db.Transaction(func(tx *gorm.DB) error {
						c0, err := co.Next(context.Background(), tx, "n0")
						if err != nil {
							ec <- err
							return err
						}

						c1, err := co.Next(context.Background(), tx, "n1")
						if err != nil {
							ec <- err
							return err
						}

						lk.Lock()
						defer lk.Unlock()

						if values["n0"] == nil {
							values["n0"] = make(map[uint32]bool)
						}
						values["n0"][c0.Value] = true

						if values["n1"] == nil {
							values["n1"] = make(map[uint32]bool)
						}
						values["n1"][c1.Value] = true

						return nil
					})
				}()
			}

			wg.Wait()

			if len(ec) > 0 {
				t.Fatal(<-ec)
			}

			for k, v := range values {
				if len(v) != nThreads {
					t.Fatalf("expected %d, got %d", nThreads, len(v))
				}

				for i := 1; i <= nThreads; i++ {
					if !v[uint32(i)] {
						t.Fatalf("missing %d in %s", i, k)
					}
				}
			}
		})
	}
}

// Counters behave correctly in the presence of rollbacks.
func TestRollback(t *testing.T) {
	const nThreads = 10

	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			// Record values
			values := make(map[uint32]bool)

			var lk sync.Mutex
			var wg sync.WaitGroup

			ec := make(chan error, nThreads)

			wg.Add(nThreads)
			for i := 0; i < nThreads; i++ {
				go func(tid int) {
					defer wg.Done()

					db.Transaction(func(tx *gorm.DB) error {
						c, err := co.Next(context.Background(), tx, "n0")
						if err != nil {
							ec <- err
							return err
						}

						// Rollback every other transaction
						if tid%2 == 0 {
							return errors.New("abort")
						}

						lk.Lock()
						defer lk.Unlock()

						values[c.Value] = true
						return nil
					})
				}(i)
			}

			wg.Wait()

			if len(ec) > 0 {
				t.Fatal(<-ec)
			}

			if len(values) != nThreads/2 {
				t.Fatalf("expected %d, got %d", nThreads/2, len(values))
			}

			for i := 1; i <= nThreads/2; i++ {
				if !values[uint32(i)] {
					t.Fatalf("missing %d", i)
				}
			}
		})
	}
}

// A counter can be deleted.
func TestDelete(t *testing.T) {
	for _, p := range parameters {
		t.Run(p.name, func(t *testing.T) {
			db, cleanup := p.initFn(t)
			defer cleanup()

			co := CounterOperator{}

			_, err := co.CreateCounter(context.Background(), db, "n0")
			if err != nil {
				t.Fatal(err)
			}

			if err := co.DeleteCounter(context.Background(), db, "n0"); err != nil {
				t.Fatal(err)
			}

			_, err = co.Next(context.Background(), db, "n0")
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func newSqliteDB(t *testing.T) (*gorm.DB, func()) {
	dir, err := os.MkdirTemp("", "test-*")
	if err != nil {
		t.Fatal(err)
	}

	db, err := gorm.Open(sqlite.Open(filepath.Join(dir, "test.db")), &gorm.Config{})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	if err = db.AutoMigrate(&Counter{}); err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	physDB, err := db.DB()
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	physDB.SetMaxOpenConns(1)

	return db, func() { os.RemoveAll(dir) }
}

func newPostgresDB(t *testing.T) (*gorm.DB, func()) {
	dsn := "user=postgres password=aide host=localhost port=5432 sslmode=disable"

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	if err = db.AutoMigrate(&Counter{}); err != nil {
		t.Fatal(err)
	}

	return db, func() { db.Migrator().DropTable(&Counter{}) }
}
