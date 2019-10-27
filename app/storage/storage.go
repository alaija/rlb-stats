package storage

import (
	"log"
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
	inMemory   InMemoryStore
	persistent PersistentStore
}

// NewStorage is new
func NewStorage(aggregateSessionDuration time.Duration, inMemory InMemoryStore, persistent PersistentStore) (store *StatsStorage, err error) {
	store = &StatsStorage{}
	store.inMemory = inMemory
	store.persistent = persistent
	store.activateAggregator(aggregateSessionDuration)

	return store, nil
}

// Save is save
func (s *StatsStorage) Save(record *StatRecord) {
	s.Lock()
	defer s.Unlock()
	err := s.inMemory.Add(record.DestHost, record.Fname)
	if err != nil {
		log.Printf("[DEBUG] save in memory failed, %s", err)
	}
}

// activateCleaner runs periodic aggregation from in memory store to bolt
func (s *StatsStorage) activateAggregator(every time.Duration) {
	ticker := time.NewTicker(every)
	now := time.Now()
	go func() {
		for range ticker.C {
			s.Lock()
			for _, d := range s.inMemory.Pop() {
				for _, r := range d.Requests {
					agr := StatAggregation{
						DestHost: d.Host,
						Fname:    r.Fname,
						Count:    r.Count,
					}
					err := s.persistent.Save(agr, now)
					if err != nil {
						log.Printf("[DEBUG] save to bolt failed, %s", err)
					}
				}
			}
			s.Unlock()
		}
	}()
}
