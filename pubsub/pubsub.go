// Package pubsub implements a channel-based publish/subscribe messaging system
// compatible with Redis Pub/Sub commands. Subscribers receive Messages via a
// buffered channel and are notified of closure through the Done channel.
package pubsub

import (
	"sync"
)

// Message represents a message published to a channel.
type Message struct {
	Data    interface{}
	Channel string
}

// Subscriber represents a client subscribed to one or more channels.
// Messages are delivered on the Messages channel. The Done channel is closed
// when the subscriber is fully unsubscribed from all channels.
type Subscriber struct {
	Messages  chan Message
	Done      chan struct{}
	ID        string
	closeOnce sync.Once
}

func (s *Subscriber) closeDone() {
	s.closeOnce.Do(func() {
		close(s.Done)
	})
}

// PubSub manages channel-based subscriptions. It maps channel names to
// subscribers and is safe for concurrent use.
type PubSub struct {
	subscribers map[string]map[string]*Subscriber
	mu          sync.RWMutex
}

// New creates and returns a new PubSub manager.
func New() *PubSub {
	return &PubSub{
		subscribers: make(map[string]map[string]*Subscriber),
	}
}

// Subscribe adds the subscriber to the given channels, creating the subscriber
// if it does not already exist. It returns the Subscriber whose Messages
// channel will receive published data. It is safe for concurrent use.
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

// Unsubscribe removes the subscriber from the given channels. If the
// subscriber is no longer subscribed to any channel, its Done channel is
// closed. It is safe for concurrent use.
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

// Publish sends data to all subscribers of channel. It returns the number
// of subscribers that successfully received the message. Subscribers with a
// full Messages buffer are skipped. It is safe for concurrent use.
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

// Pattern associates a glob-style pattern with a Subscriber.
type Pattern struct {
	Subscriber *Subscriber
	Pattern    string
}

// PatternPubSub extends PubSub with pattern-based subscriptions.
type PatternPubSub struct {
	patterns map[string]map[string]*Pattern
	PubSub
}

// NewPattern creates and returns a new PatternPubSub manager.
func NewPattern() *PatternPubSub {
	return &PatternPubSub{
		PubSub:   *New(),
		patterns: make(map[string]map[string]*Pattern),
	}
}

// PSubscribe registers pattern subscriptions for subscriberID. The returned
// Subscriber will receive messages published to channels matching any of the
// patterns. It is safe for concurrent use.
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

// PUnsubscribe removes pattern subscriptions for subscriberID. If the
// subscriber is no longer subscribed to any pattern, its Done channel is
// closed. It is safe for concurrent use.
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
