package contentconsumer

import (
	"time"
)

//ConsumerEvent - Events
type ConsumerEvent struct {
	when time.Time
	ID   string
	Err  error
}

//ConsumerEventPtr - Ptr to ConsumerEvent
type ConsumerEventPtr *ConsumerEvent

//ConsumerEventConsumer -
type ConsumerEventConsumer interface {
	//Channel - Return Channel to write to.  nil if channel is closed
	Channel() chan<- ConsumerEventPtr
	//Signal Close of channel when writing is finished
	CloseChannel()
}
