package notify

import (
	"context"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("notification")

type Notification struct {
	// endOfFileEvent is used to notify when a file download is completed
	endOfFileEvent chan struct{}
	// finished is used to signal when the `handleEndOfFileEvent` callback function has finished its execution
	finished chan struct{}
}

// NewNotification returns a new Notification
func NewNotification() *Notification {
	return &Notification{
		endOfFileEvent: make(chan struct{}),
		finished:       make(chan struct{}),
	}
}

// ListenEndOfFile listens for file download completion events. Upon receiving an event, it will trigger the corresponding
// callback method to handle the event `handleEndOfFileEvent`. Once the callback method finishes processing,
// a notification will be sent to the `finished` channel, signaling the completion of the event handling.
func (n *Notification) ListenEndOfFile(ctx context.Context, handleEndOfFileEvent func() error) {
	for {
		select {
		case <-n.endOfFileEvent:
			if err := handleEndOfFileEvent(); err != nil {
				log.Errorf("handle endOfFile event failed: %v", err)
			}
			n.finished <- struct{}{}
		case <-ctx.Done():
			return
		}
	}
}

// SendEndOfFileEvent notifies a file download is completed.
func (n *Notification) SendEndOfFileEvent() {
	n.endOfFileEvent <- struct{}{}

	select {
	case <-n.finished:
	}
}
