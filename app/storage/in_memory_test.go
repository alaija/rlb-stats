package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// creates inMemory and adds two records to a destination
func prepareInMemory(t *testing.T) *InMemory {

	s := NewInMemory()

	err := s.Add(host, fname)
	assert.NoError(t, err)
	d, err := s.Get(host)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(d.Requests))
	assert.Equal(t, 1, d.Requests[fname].Count)

	err = s.Add(host, fname)
	assert.NoError(t, err)
	d, err = s.Get(host)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(d.Requests))
	assert.Equal(t, 2, d.Requests[fname].Count)

	err = s.Add(host, fname2)
	assert.NoError(t, err)
	d, err = s.Get(host)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(d.Requests))
	assert.Equal(t, 1, d.Requests[fname2].Count)

	err = s.Add(host2, fname2)
	assert.NoError(t, err)
	d, err = s.Get(host2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(d.Requests))
	assert.Equal(t, 1, d.Requests[fname2].Count)

	return s
}

func Test_InMemory_Clear(t *testing.T) {
	s := prepareInMemory(t)

	err := s.Clear(host)
	assert.NoError(t, err)

	d, err := s.Get(host)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(d.Requests))
}

func Test_InMemory_Delete(t *testing.T) {
	s := prepareInMemory(t)

	err := s.Delete(host)
	assert.NoError(t, err)

	d, err := s.Get(host)
	assert.Equal(t, errors.New("Destination can't be found"), err)
	assert.Nil(t, d)

	err = s.Delete(host)
	assert.Equal(t, errors.New("Destination can't be found"), err)

	err = s.Clear(host)
	assert.Equal(t, errors.New("Destination can't be found"), err)
}

func Test_InMemory_Pop(t *testing.T) {
	s := prepareInMemory(t)

	dests := s.Pop()
	assert.Equal(t, 2, len(dests))
	dest := dests[0]
	assert.Equal(t, 2, len(dest.Requests))
	assert.Equal(t, 2, dest.Requests[fname].Count)

	d, err := s.Get(host)
	assert.Equal(t, errors.New("Destination can't be found"), err)
	assert.Nil(t, d)

	err = s.Delete(host)
	assert.Equal(t, errors.New("Destination can't be found"), err)

	err = s.Clear(host)
	assert.Equal(t, errors.New("Destination can't be found"), err)
}
