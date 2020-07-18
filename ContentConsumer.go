package contentconsumer

import (
	"container/ring"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

//ChunkContent - Payload of 1 chunk of content
// tVal - t value in manifest
// dVal - duration of the content
// data - actual payload
type ChunkContent struct {
	dur  time.Duration
	data []byte
}

//ChunkContentPtr - Allocated Chunk Payload
type ChunkContentPtr *ChunkContent

//NewChunkContent - Create new Chunk
func NewChunkContent(dur time.Duration, data []byte) ChunkContentPtr {
	ret := new(ChunkContent)
	ret.dur = dur
	ret.data = data
	return ret
}

//ContentConsumer - Consumer of the content
//Consumes the Chunks sent on inputChan
//Uses Duration of content to indicate buffer level
//Checks Clock and drains the buffer
type ContentConsumer struct {
	id                  string
	alive               bool
	inChannelOpen       bool
	writerOwner         *ring.Ring
	readerReferer       *ring.Ring
	buffersLock         sync.RWMutex
	inputChan           chan ChunkContentPtr
	lastTimeEval        time.Time
	consumptionInterval time.Duration
	bufferAvailPeriod   time.Duration
	postDataFunc        func(time.Duration, []byte)
}

//NewContentConsumer - Create
func NewContentConsumer(id string, slots int) *ContentConsumer {
	ret := &ContentConsumer{}
	ret.id = id
	ret.inputChan = make(chan ChunkContentPtr, slots)
	ret.writerOwner = ring.New(slots)
	// Iterate through the ring and print its contents
	count := 0
	for count < ret.writerOwner.Len() {
		ret.writerOwner = ret.writerOwner.Next()
		ret.writerOwner.Value = nil
		count++
	}
	ret.readerReferer = ret.writerOwner
	ret.inChannelOpen = true
	ret.consumptionInterval = 1 * time.Second
	ret.bufferAvailPeriod = 0 * time.Second
	ret.postDataFunc = nil
	return ret
}

//SetPostDataFunc - Hook to handle the data post
func (cc *ContentConsumer) SetPostDataFunc(f func(time.Duration, []byte)) {
	cc.postDataFunc = f
}

//GetChannel - Returns the channel to write to
func (cc *ContentConsumer) GetChannel() chan ChunkContentPtr {
	if cc.inChannelOpen {
		return cc.inputChan
	}
	return nil
}

//CloseChannel - Close the input channel
// This will make the Go Routine Run exit
func (cc *ContentConsumer) CloseChannel() {
	if cc.inChannelOpen {
		close(cc.inputChan)
		cc.inChannelOpen = false
	}
}

//SetConsumptionInterval - Set the consumption interval from buffers
func (cc *ContentConsumer) SetConsumptionInterval(d time.Duration) {
	cc.consumptionInterval = d
}

//addchunk - Add a incoming Chunk
func (cc *ContentConsumer) addchunk(c ChunkContentPtr) error {
	cc.buffersLock.Lock()
	defer cc.buffersLock.Unlock()
	//For now ignore the buffer
	cc.writerOwner = cc.writerOwner.Next()
	if cc.writerOwner.Value == nil {
		cc.writerOwner.Value = c
	} else {
		return fmt.Errorf("Not Enough buffer. Ignoring chunk. %d", cc.writerOwner.Len())
	}
	cc.bufferAvailPeriod += c.dur
	return nil
}

//consumeNextChunk - Consume Next Chunk from last poll to now
//elapsed - Actual Time worth of content required
//Return:
//  remain elapsed - Amount of content not given
//  error - If content is not available
func (cc *ContentConsumer) consumeNextChunk(elapsed time.Duration) (time.Duration, error) {
	cc.buffersLock.Lock()
	defer cc.buffersLock.Unlock()
	count := 0
	validEnd := false
	for count < cc.readerReferer.Len() {
		cc.readerReferer = cc.readerReferer.Next()
		count++
		if cc.readerReferer.Value == nil {
			continue
		}
		var chunk ChunkContentPtr
		chunk = cc.readerReferer.Value.(ChunkContentPtr)
		if chunk == nil {
			//Bad data type assigned...
			continue
		}
		if chunk.dur < elapsed {
			//chunk fully consumed
			cc.readerReferer.Value = nil
			elapsed = elapsed - chunk.dur
			cc.bufferAvailPeriod -= chunk.dur
			if cc.postDataFunc != nil {
				go cc.postDataFunc(chunk.dur, chunk.data)
			}
		} else if chunk.dur == elapsed {
			//chunk fully consumed
			cc.readerReferer.Value = nil
			cc.bufferAvailPeriod -= chunk.dur
			validEnd = true
			elapsed = 0
			if cc.postDataFunc != nil {
				go cc.postDataFunc(chunk.dur, chunk.data)
			}
			break
		} else {
			//elapsed is < chunkDur
			//Don't consume the chunk...
			validEnd = true
			break
		}
	}
	if !validEnd {
		//Not enough to drain
		return elapsed, fmt.Errorf("Not Enough drain. Under by %v", elapsed)
	}
	return elapsed, nil
}

//DrainChunks - Drain the actual content
func (cc *ContentConsumer) DrainChunks(wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}
	var remain time.Duration
	var err error
	for cc.alive {
		select {
		case <-time.After(cc.consumptionInterval):
			curTime := time.Now()
			elapsed := curTime.Sub(cc.lastTimeEval)
			if remain > 0 {
				elapsed = elapsed + remain
				remain = 0
			}
			cc.lastTimeEval = curTime
			remain, err = cc.consumeNextChunk(elapsed)
			if err != nil {
				log.Printf("Error Consuming Chunk: %s Remain: %v", err.Error(), remain)
			}
			runtime.Gosched()
		}
	}
}

//Run - Start Consuming Content
func (cc *ContentConsumer) Run(wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
		defer wg.Done()
	}
	drainChunksStarted := false
	cc.alive = true
	for cc.alive {
		select {
		case chunk, channelOpen := <-cc.inputChan:
			if chunk != nil {
				//add chunk
				cc.addchunk(chunk)
				runtime.Gosched()
				if !drainChunksStarted {
					//On first chunk arrival start draining
					cc.lastTimeEval = time.Now()
					go cc.DrainChunks(wg)
					drainChunksStarted = true
				}
			} else {
				//Error in channel
				if !channelOpen {
					//Channel closed
					cc.alive = false
					break
				}
			}
		}
	}
}
