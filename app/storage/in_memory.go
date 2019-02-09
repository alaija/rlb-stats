package storage

import (
	"errors"
	"sync"
)

// InMemory struct describes Destination storage
// the key is destination host
type InMemory struct {
	sync.RWMutex
	destinations map[string]*Destination
}

type (
	// FileRequest model describes file request metadata
	FileRequest struct {
		Fname string
		Count int
	}
	// Destination model describes destination with mutiple FileRequests
	Destination struct {
		Host     string
		Requests map[string]*FileRequest
	}
)

// NewInMemory creates new instance of LogSotrage
func NewInMemory() *InMemory {
	s := &InMemory{}
	s.destinations = make(map[string]*Destination)

	return s
}

// Add sets file request to storage by destination host
func (m *InMemory) Add(host string, fname string) {
	m.Lock()
	defer m.Unlock()

	destination, ok := m.destinations[host]

	if ok == false {
		m.destinations[host] = &Destination{
			host,
			map[string]*FileRequest{fname: &FileRequest{fname, 1}},
		}
		return
	}

	request, ok := destination.Requests[fname]

	if !ok {
		request = &FileRequest{fname, 0}
		destination.Requests[fname] = request
	}

	request.Count++
}

// Delete removes destination from storage by host
// returns error if nothing found
func (m *InMemory) Delete(host string) error {
	m.Lock()
	defer m.Unlock()

	_, ok := m.destinations[host]

	if !ok {
		return errors.New("Destination can't be found")
	}

	delete(m.destinations, host)
	return nil
}

// Clear removes file requests from storage by host
// returns error if host can't be found
func (m *InMemory) Clear(host string) error {
	m.Lock()
	defer m.Unlock()

	destination, ok := m.destinations[host]

	if !ok {
		return errors.New("Destination can't be found")
	}

	destination.Requests = make(map[string]*FileRequest)
	return nil
}

// Get gets destination from storage by host
// returns an error if nothing found
func (m *InMemory) Get(host string) (destination *Destination, err error) {
	m.RLock()
	defer m.RUnlock()

	destination, ok := m.destinations[host]

	if ok == false {
		return nil, errors.New("Destination can't be found")
	}

	return destination, nil
}

// Pop gets and removes all data from storage
func (m *InMemory) Pop() (destinations []Destination) {
	m.RLock()
	defer m.RUnlock()

	for _, d := range m.destinations {
		destinations = append(destinations, *d)
	}
	m.destinations = make(map[string]*Destination)

	return destinations
}
