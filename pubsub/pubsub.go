package pubsub

import (
	"sync"
)

// Message represents a message in the pub/sub system
type Message struct {
	Channel string
	Data    interface{}
}

// Subscriber represents a subscriber to one or more channels
type Subscriber struct {
	ID        string
	Messages  chan Message
	Done      chan struct{}
	closeOnce sync.Once
}

func (s *Subscriber) closeDone() {
	s.closeOnce.Do(func() {
		close(s.Done)
	})
}

// PubSub represents the pub/sub system
type PubSub struct {
	mu          sync.RWMutex
	subscribers map[string]map[string]*Subscriber // channel -> subscriberID -> subscriber
}

// New creates a new PubSub instance
func New() *PubSub {
	return &PubSub{
		subscribers: make(map[string]map[string]*Subscriber),
	}
}

// Subscribe subscribes to one or more channels
func (ps *PubSub) Subscribe(subscriberID string, channels ...string) *Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	var subscriber *Subscriber
	// Try to find existing subscriber with the same ID
	for _, subs := range ps.subscribers {
		if sub, ok := subs[subscriberID]; ok {
			subscriber = sub
			break
		}
	}

	if subscriber == nil {
		subscriber = &Subscriber{
			ID:       subscriberID,
			Messages: make(chan Message, 100), // Buffer size of 100
			Done:     make(chan struct{}),
		}
	}

	for _, channel := range channels {
		if _, ok := ps.subscribers[channel]; !ok {
			ps.subscribers[channel] = make(map[string]*Subscriber)
		}
		ps.subscribers[channel][subscriberID] = subscriber
	}

	return subscriber
}

// Unsubscribe unsubscribes from one or more channels
func (ps *PubSub) Unsubscribe(subscriberID string, channels ...string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, channel := range channels {
		if subs, ok := ps.subscribers[channel]; ok {
			if sub, ok := subs[subscriberID]; ok {
				delete(subs, subscriberID)
				// Only close Done if subscriber is no longer in any channel
				stillSubscribed := false
				for _, otherSubs := range ps.subscribers {
					if _, ok := otherSubs[subscriberID]; ok {
						stillSubscribed = true
						break
					}
				}
				if !stillSubscribed {
					sub.closeDone()
				}
			}
			if len(subs) == 0 {
				delete(ps.subscribers, channel)
			}
		}
	}
}

// Publish publishes a message to a channel and returns the number of subscribers that received it
func (ps *PubSub) Publish(channel string, data interface{}) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	count := 0
	if subs, ok := ps.subscribers[channel]; ok {
		message := Message{
			Channel: channel,
			Data:    data,
		}

		for _, sub := range subs {
			select {
			case <-sub.Done:
				// Subscriber is done, skip
				continue
			case sub.Messages <- message:
				// Message sent successfully
				count++
			default:
				// Channel is full, skip this subscriber
			}
		}
	}
	return count
}

// Pattern represents a pattern subscription
type Pattern struct {
	Pattern    string
	Subscriber *Subscriber
}

// PatternPubSub represents the pattern-based pub/sub system
type PatternPubSub struct {
	PubSub
	patterns map[string]map[string]*Pattern // pattern -> subscriberID -> pattern
}

// NewPattern creates a new PatternPubSub instance
func NewPattern() *PatternPubSub {
	return &PatternPubSub{
		PubSub:   *New(),
		patterns: make(map[string]map[string]*Pattern),
	}
}

// PSubscribe subscribes to channels matching the given patterns
func (pps *PatternPubSub) PSubscribe(subscriberID string, patterns ...string) *Subscriber {
	pps.mu.Lock()
	defer pps.mu.Unlock()

	subscriber := &Subscriber{
		ID:       subscriberID,
		Messages: make(chan Message, 100),
		Done:     make(chan struct{}),
	}

	for _, pattern := range patterns {
		if _, ok := pps.patterns[pattern]; !ok {
			pps.patterns[pattern] = make(map[string]*Pattern)
		}
		pps.patterns[pattern][subscriberID] = &Pattern{
			Pattern:    pattern,
			Subscriber: subscriber,
		}
	}

	return subscriber
}

// PUnsubscribe unsubscribes from the given patterns
func (pps *PatternPubSub) PUnsubscribe(subscriberID string, patterns ...string) {
	pps.mu.Lock()
	defer pps.mu.Unlock()

	for _, pattern := range patterns {
		if subs, ok := pps.patterns[pattern]; ok {
			if pat, ok := subs[subscriberID]; ok {
				delete(subs, subscriberID)
				// Only close Done if subscriber is no longer in any pattern
				stillSubscribed := false
				for _, otherSubs := range pps.patterns {
					if _, ok := otherSubs[subscriberID]; ok {
						stillSubscribed = true
						break
					}
				}
				if !stillSubscribed {
					pat.Subscriber.closeDone()
				}
			}
			if len(subs) == 0 {
				delete(pps.patterns, pattern)
			}
		}
	}
}
