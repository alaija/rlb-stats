package storage

import (
	"sync"
	"time"
)

// StatRecord model describes stats  metadata
type StatRecord struct {
	DestHost string
	Fname    string
}

// StatsStorage is storage
type StatsStorage struct {
	sync.Mutex
	inMemory *InMemory
	bolt     *Bolt
}

// NewStorage is new
func NewStorage(aggregateSessionDuration time.Duration, dbFile string, durationBuckets []string) (store *StatsStorage, err error) {
	store = &StatsStorage{}
	store.inMemory = NewInMemory()
	store.bolt, err = NewBolt(dbFile, durationBuckets)
	if err != nil {
		return nil, err
	}
	store.activateAggregator(aggregateSessionDuration)

	return store, nil
}

// Save is save
func (s *StatsStorage) Save(record *StatRecord) {
	s.Lock()
	s.inMemory.Add(record.DestHost, record.Fname)
	s.Unlock()
}

// activateCleaner runs periodic aggregation from in memory store to bolt
func (s *StatsStorage) activateAggregator(every time.Duration) {
	ticker := time.NewTicker(every)
	now := time.Now()
	go func() {
		s.Lock()
		defer s.Unlock()
		for range ticker.C {
			for _, d := range s.inMemory.Pop() {
				for _, r := range d.Requests {
					agr := StatAggregation{
						DestHost: d.Host,
						Fname:    r.Fname,
						Count:    r.Count,
					}
					s.bolt.Save(agr, now)
				}
			}
		}
	}()
}
