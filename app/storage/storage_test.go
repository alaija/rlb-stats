package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const agregatorTimeout = 100 * time.Millisecond

func Test_StatsStorage_ActivateAggregator(t *testing.T) {
	defer os.Remove(testDB)
	s := prepareStorage(t)
	assert.NotNil(t, s)

	time.Sleep(agregatorTimeout)
}

func prepareStorage(t *testing.T) *StatsStorage {
	os.Remove(testDB)

	buckets := []string{
		"1m",
		"15m",
		"1h",
		"24h",
	}

	s, err := NewStorage(agregatorTimeout, testDB, buckets)
	assert.NoError(t, err)

	r := &StatRecord{
		Fname:    fname,
		DestHost: host,
	}
	s.Save(r)

	return s
}
