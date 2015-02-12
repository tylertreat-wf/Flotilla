package vessel

import (
	"fmt"

	"github.com/Workiva/go-vessel"

	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

const channel = "global:test"

// Peer implements the peer interface for Vessel brokers.
type Peer struct {
	transport  vessel.Transport
	messages   chan []byte
	send       chan []byte
	errors     chan error
	done       chan bool
	stop       chan bool
	subscriber bool
}

// NewPeer creates and returns a new Peer for communicating with Vessel
// brokers.
func NewPeer(host string) (*Peer, error) {
	clientID := broker.GenerateName()
	transport := vessel.NewTCPTransport(clientID)
	if err := transport.Dial(fmt.Sprintf("http://%s/vessel", host)); err != nil {
		return nil, err
	}

	return &Peer{
		transport: transport,
		messages:  make(chan []byte, 10000),
		send:      make(chan []byte),
		errors:    make(chan error, 1),
		done:      make(chan bool),
		stop:      make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (p *Peer) Subscribe() error {
	p.subscriber = true
	subscription, err := p.transport.Subscribe(channel)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case msg := <-subscription:
				select {
				case p.messages <- msg:
				default:
					<-p.stop
					return
				}
			case <-p.stop:
				return
			}
		}
	}()

	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (p *Peer) Recv() ([]byte, error) {
	return <-p.messages, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (p *Peer) Send() chan<- []byte {
	return p.send
}

// Errors returns the channel on which the peer sends publish errors.
func (p *Peer) Errors() <-chan error {
	return p.errors
}

// Done signals to the peer that message publishing has completed.
func (p *Peer) Done() {
	p.done <- true
}

// Setup prepares the peer for testing.
func (p *Peer) Setup() {
	go func() {
		for {
			select {
			case msg := <-p.send:
				if err := p.transport.Publish(channel, msg); err != nil {
					p.errors <- err
				}
			case <-p.done:
				return
			}
		}
	}()
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (p *Peer) Teardown() {
	if p.subscriber {
		p.stop <- true
	}
	p.transport.Close()
}
