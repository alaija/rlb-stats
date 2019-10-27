package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	fname  = "123.mp3"
	fname1 = "321.mp3"
	fname2 = "4.mp3"

	host  = "https://test.com"
	host1 = "https://test1.com"
	host2 = "https://test2.com"

	agregatorTimeout = 100 * time.Millisecond
)

func Test_StatsStorage_Save(t *testing.T) {
	inMemory := MockInMemoryStore{}
	persistent := MockPersistentStore{}
	s := prepareStorage(t, &inMemory, &persistent)
	assert.NotNil(t, s)

	r := &StatRecord{
		Fname:    fname,
		DestHost: host,
	}
	inMemory.On("Add", r.DestHost, r.Fname).Return(nil)
	inMemory.On("Pop").Return(nil)
	s.Save(r)
	inMemory.AssertCalled(t, "Add", r.DestHost, r.Fname)
}

func Test_StatsStorage_ActivateAggregator_EmptyInMemory(t *testing.T) {
	inMemory := MockInMemoryStore{}
	persistent := MockPersistentStore{}
	s := prepareStorage(t, &inMemory, &persistent)
	assert.NotNil(t, s)

	inMemory.On("Pop").Return(nil)
	time.Sleep(2 * agregatorTimeout)
	inMemory.AssertCalled(t, "Pop")
	persistent.AssertNotCalled(t, "Save")
}

func Test_StatsStorage_ActivateAggregator_NonEmptyInMemory(t *testing.T) {
	inMemory := MockInMemoryStore{}
	persistent := MockPersistentStore{}
	s := prepareStorage(t, &inMemory, &persistent)
	assert.NotNil(t, s)

	agr := StatAggregation{
		Fname:    fname,
		DestHost: host,
		Count:    1,
	}

	fileRequests := map[string]*FileRequest{
		host: &FileRequest{
			Fname: fname,
			Count: 1,
		},
	}
	destination := Destination{
		Host:     host,
		Requests: fileRequests,
	}

	inMemory.On("Pop").Return([]Destination{destination})
	persistent.On("Save", agr, mock.Anything).Return(nil)
	time.Sleep(2 * agregatorTimeout)
	inMemory.AssertExpectations(t)
	persistent.AssertExpectations(t)
}
func prepareStorage(t *testing.T, inMemory InMemoryStore, persistent PersistentStore) *StatsStorage {
	s, err := NewStorage(agregatorTimeout, inMemory, persistent)
	assert.NoError(t, err)

	return s
}
