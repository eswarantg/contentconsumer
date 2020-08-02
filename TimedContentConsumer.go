//Package contentconsumer - Consumes content from channel.  (e.g.) TimedContentConsumer
package contentconsumer

import (
	"container/ring"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

//TimedContent - Content with duration
// duration - content duration
// data - actual content
type TimedContent struct {
	duration time.Duration
	data     interface{} //any data
}

//TimedContentPtr - Ptr to the TimedContext
type TimedContentPtr *TimedContent

//NewTimedContent - Create new TimedContent
func NewTimedContent(duration time.Duration, data interface{}) TimedContentPtr {
	ret := new(TimedContent)
	ret.duration = duration
	ret.data = data
	return ret
}

//ErrConsumerDrainedEmpty - Empty Consumer Buffer
var ErrConsumerDrainedEmpty error = errors.New("EmptyBuffer")

// ErrConsumerChannelClosed - AddChunk to closed channel
var ErrConsumerChannelClosed error = errors.New("ChannelClosed")

//TimedContentConsumer -
//   * Maintains a Cache of given ConsumptionInterval
//       * Channel may have more
//   * Drains the Cache in fixed intervals
//       * While Draining can forward data using PostDataFunc(duration, data)
//   * If CacheBuffer below Min Level - calls BelowMinLevelFunc()
type TimedContentConsumer struct {

	//EXTERNAL:

	//id - User name for the TimedContentConsumer
	// used in the logging
	ID string
	//Interval to drain buffer for content worth the interval
	ConsumptionInterval time.Duration

	//downstream Consumer of content drained
	Downstream chan<- interface{}
	//downstream Consumer of events
	EventDownstream chan<- ConsumerEventPtr

	//INTERNAL:

	//inputChan - Channel where Timed Content is fed to the consumer
	inputChan chan TimedContentPtr
	//inChannelOpen - holds tate of if inputChan is open, for safety
	inChannelOpen bool
	//chanMutex - mutex to stop channel close race
	inputChanMutex sync.Mutex

	//buffer - Ring buffer to hold the content worth
	buffer *ring.Ring
	//Mutex gaurding the buffer
	buffersLock sync.Mutex
	//Reference to the buffer for writing -> Place to write next item
	writer *ring.Ring
	//Reference to the buffer for reading -> Place to read next item
	reader *ring.Ring
	//Amount of content in buffer
	bufferAvailPeriod time.Duration
	//Time on wall clock, last time buffer was drained
	lastTimeEval time.Time
}

//EXTERNAL

//NewTimedContentConsumer - Create
//  id - User Name
//  interval - Duration of poll
//  slots - intial ring buffer size estimate content to fill
//          No worries it will be doubled if required
func NewTimedContentConsumer(id string, interval time.Duration, slots int) *TimedContentConsumer {
	ret := &TimedContentConsumer{}
	ret.ID = id

	ret.inputChan = make(chan TimedContentPtr, slots)
	ret.inChannelOpen = true

	//Minimum 10 slots
	if slots < 10 {
		slots = 10
	}

	ret.buffer = ring.New(slots)
	ret.writer = ret.buffer        //Current write location.. Write() and Next()
	ret.reader = ret.buffer.Prev() //Last Read location.  Next() and Read()

	ret.ConsumptionInterval = interval

	ret.Downstream = nil
	ret.EventDownstream = nil
	return ret
}

//Channel - Returns the channel to write to
func (cc *TimedContentConsumer) Channel() chan<- TimedContentPtr {
	cc.inputChanMutex.Lock()
	defer cc.inputChanMutex.Unlock()
	if cc.inChannelOpen {
		return cc.inputChan
	}
	return nil
}

//CloseChannel - Close the input channel
// This will make the Go Routine Run exit
func (cc *TimedContentConsumer) CloseChannel() {
	cc.inputChanMutex.Lock()
	defer cc.inputChanMutex.Unlock()
	if cc.inChannelOpen {
		close(cc.inputChan)
		cc.inChannelOpen = false
	}
}

//INTERNAL

//addChunk - Add a incoming Chunk
func (cc *TimedContentConsumer) addChunk(c TimedContentPtr) error {
	cc.buffersLock.Lock()
	defer cc.buffersLock.Unlock()
	if c == nil || c.data == nil {
		return ErrConsumerChannelClosed
	}
	//If current writer position is not empty
	//and reader is just behind to read
	if cc.writer.Value != nil && cc.writer == cc.reader.Next() {
		//log.Printf("Extending Ring Start %v %v", cc.writer, cc.writer.Len())
		//log.Printf("WRITER VIEW")
		//cc.printRingBuf(cc.writer)
		//log.Printf("READER VIEW")
		//cc.printRingBuf(cc.reader.Next())
		//Ring is full.. Need to extend
		extendRing := ring.New(cc.buffer.Len())
		//log.Printf("EXTENDED VIEW")
		//cc.printRingBuf(extendRing)
		cc.reader = cc.reader.Link(extendRing).Prev()
		cc.writer = extendRing
		//log.Printf("WRITER VIEW")
		//cc.printRingBuf(cc.writer)
		//log.Printf("READER VIEW")
		//cc.printRingBuf(cc.reader.Next())
	}
	cc.writer.Value = c
	cc.writer = cc.writer.Next()
	cc.bufferAvailPeriod += c.duration
	return nil
}

//consumeChunks - Consume Chunks for Elapsed Duration
//elapsed - Actual Time worth of content required
//Return:
//  remain elapsed - Amount of content not given
//  error - If content is not available
func (cc *TimedContentConsumer) consumeChunks(elapsed time.Duration) (time.Duration, error) {
	cc.buffersLock.Lock()
	defer cc.buffersLock.Unlock()
	for {
		if cc.reader.Next() == cc.writer {
			//Nothing to read
			//log.Printf("Noting READ... ")
			return elapsed, ErrConsumerDrainedEmpty
		}
		//Step forward for reading
		cc.reader = cc.reader.Next()
		if cc.reader.Value == nil {
			//Should not happen... bad types
			panic("Bad Data in Ring Buffer.")
		}
		var chunk TimedContentPtr
		chunk = cc.reader.Value.(TimedContentPtr)
		if chunk == nil {
			//Should not happen... bad type
			panic("Bad Data in Ring Buffer.")
		}
		if chunk.duration < elapsed {
			//chunk fully consumed
			cc.reader.Value = nil
			elapsed = elapsed - chunk.duration
			cc.bufferAvailPeriod -= chunk.duration
			if cc.Downstream != nil {
				cc.Downstream <- chunk.data
				//log.Printf("SENT %v %v", chunk.data, chunk.duration)
			}
			continue //next chunk also required
		} else if chunk.duration == elapsed {
			//chunk fully consumed
			cc.reader.Value = nil
			cc.bufferAvailPeriod -= chunk.duration
			elapsed = 0
			if cc.Downstream != nil {
				cc.Downstream <- chunk.data
				//log.Printf("SENT %v %v", chunk.data, chunk.duration)
			}
			break //All data read fully... exit
		} else {
			//elapsed is < chunkDur
			//Don't consume the chunk...

			//Retain reading to same buffer
			//Step back for re-read
			cc.reader = cc.reader.Prev()
			break
		}
	}
	return elapsed, nil
}

//DrainChunks - Drain the actual content
func (cc *TimedContentConsumer) DrainChunks(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}
	defer func() {
		if cc.Downstream != nil {
			close(cc.Downstream)
			cc.Downstream = nil
			//log.Printf("Closed Downstream")
		}
		if cc.EventDownstream != nil {
			close(cc.EventDownstream)
			cc.EventDownstream = nil
			//log.Printf("Closed EventDownstream")
		}
	}()

	var remain time.Duration
	var err error
	for {
		select {
		case <-ctx.Done():
			//Context is cancelled ... exit..
			return
		case <-time.After(cc.ConsumptionInterval):
			curTime := time.Now()
			elapsed := curTime.Sub(cc.lastTimeEval)
			if remain > 0 {
				elapsed = elapsed + remain
				remain = 0
			}
			cc.lastTimeEval = curTime
			remain, err = cc.consumeChunks(elapsed)
			if err != nil {
				if cc.EventDownstream != nil {
					cc.EventDownstream <- &ConsumerEvent{time.Now(), cc.ID, fmt.Errorf("Elapsed %v: %w", remain, err)}
					//log.Printf("ERR SENT %v %v %v", cc.ID, remain, err)
				}
			}
			runtime.Gosched()
		}
	}
}

//Run - Start Consuming Content
func (cc *TimedContentConsumer) Run(wg *sync.WaitGroup) {
	var cleanup func()
	if wg != nil {
		cleanup = func() {
			wg.Done()
		}
	} else {
		cleanup = func() {}
	}
	defer cleanup()
	//Create a cancellable concext for all related stuff to stop at end of this
	ctx, cancelfunc := context.WithCancel(context.Background())
	defer cancelfunc()

	drainChunksStarted := false
	for {
		select {
		case chunk, channelOpen := <-cc.inputChan:
			if chunk != nil {
				//add chunk
				cc.addChunk(chunk)
				if !drainChunksStarted {
					//On first chunk arrival start draining
					cc.lastTimeEval = time.Now()
					go cc.DrainChunks(ctx, wg)
					drainChunksStarted = true
				}
				runtime.Gosched()
			} else {
				//Error in channel
				if !channelOpen {
					//Channel closed
					return
				}
			}
		}
	}
}
