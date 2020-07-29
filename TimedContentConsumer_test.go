package contentconsumer

import (
	"container/ring"
	"log"
	"sync"
	"testing"
	"time"
)

//Helper function for test - Begin
func addTestChunks(t *testing.T, cc *TimedContentConsumer, dur time.Duration, repeat int, val int) int {
	for i := 0; i < repeat; i++ {
		err := cc.addChunk(NewTimedContent(dur, val))
		val++
		if err != nil {
			t.Errorf("%s", err.Error())
			return val
		}
		t.Logf("Added %v worth content, Tot: %v", dur, cc.bufferAvailPeriod)
	}
	return val
}
func goodRemoveTestChunks(t *testing.T, cc *TimedContentConsumer, dur time.Duration, expRemain time.Duration) {
	remain, err := cc.consumeChunks(dur)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	if remain != expRemain {
		t.Errorf("Remaining Exp:%v Act:%v", expRemain, remain)
	}
	t.Logf("Removed %v worth content, Remain: %v, Tot: %v", dur, remain, cc.bufferAvailPeriod)
}
func badRemoveTestChunks(t *testing.T, cc *TimedContentConsumer, dur time.Duration) {
	remain, err := cc.consumeChunks(dur)
	if err != nil {
		t.Logf("%s", err.Error())
		return
	}
	t.Errorf("Removed %v worth content, Remain %v, Tot: %v", dur, remain, cc.bufferAvailPeriod)
}

func testBufferLen(t *testing.T, cc *TimedContentConsumer, dur time.Duration) {
	if cc.bufferAvailPeriod != dur {
		t.Errorf("Content worth mismatch, Exp: %v Act: %v", dur, cc.bufferAvailPeriod)
	}
}

func printRingBuf(r *ring.Ring) {
	for i := 0; i < r.Len(); i++ {
		if r.Value != nil {
			ptr := r.Value.(TimedContentPtr)
			switch v := ptr.data.(type) {
			case int:
				log.Printf("%v %v %d", r, ptr.duration, v)
			case []byte:
				log.Printf("%v %v %d", r, ptr.duration, len(v))
			}

		} else {
			log.Printf("%v <nil>", r)
		}
		r = r.Next()
	}
}

//Helper function for test - End
func TestAddNullData(t *testing.T) {
	cc := NewTimedContentConsumer("client1", 1*time.Second, 1)
	err := cc.addChunk(NewTimedContent(1*time.Second, nil))
	if err != nil {
		t.Logf("%s", err.Error())
	} else {
		t.Errorf("Error: <nil> data allowed to be added")
	}
}

func TestDrainPositive(t *testing.T) {
	var dur time.Duration
	var expdur time.Duration
	var expremain time.Duration
	ringLen := 5
	cc := NewTimedContentConsumer("client1", 1*time.Second, ringLen)
	//Min default is 10... test for it
	if cc.writer.Len() != 10 {
		t.Errorf("Ring length not matching Exp:%v Act:%v", 10, cc.writer.Len())
		return
	}
	cc.lastTimeEval = time.Now()
	//0
	i := 0
	dur = 1 * time.Second
	expdur += 10 * dur
	i = addTestChunks(t, cc, dur, 10, i)
	testBufferLen(t, cc, expdur)
	//10
	dur = 2 * time.Second
	expdur += 3 * dur
	i = addTestChunks(t, cc, dur, 3, i)
	testBufferLen(t, cc, expdur)
	//16
	dur = 7 * time.Second
	expremain = 0 * time.Second
	goodRemoveTestChunks(t, cc, dur, expremain)
	expdur -= dur - expremain
	testBufferLen(t, cc, expdur)
	//9
	dur = 2 * time.Second
	expremain = 0 * time.Second
	goodRemoveTestChunks(t, cc, dur, expremain)
	expdur -= dur - expremain
	testBufferLen(t, cc, expdur)
	//7
	dur = 7 * time.Second
	expremain = 0 * time.Second
	goodRemoveTestChunks(t, cc, dur, expremain)
	expdur -= dur - expremain
	testBufferLen(t, cc, expdur)
	//0
	dur = 1 * time.Second
	badRemoveTestChunks(t, cc, dur)
	testBufferLen(t, cc, expdur)
	//0
	dur = 4 * time.Second
	expdur += 1 * dur
	i = addTestChunks(t, cc, dur, 1, i)
	testBufferLen(t, cc, expdur)
	//4
	dur = 2 * time.Second
	expremain = 2 * time.Second
	goodRemoveTestChunks(t, cc, dur, expremain)
	expdur -= dur - expremain
	testBufferLen(t, cc, expdur)
	//4
	dur = 4 * time.Second
	expremain = 0 * time.Second
	goodRemoveTestChunks(t, cc, dur, expremain)
	expdur -= dur - expremain
	testBufferLen(t, cc, expdur)
	//0
}

func TestFeed(t *testing.T) {
	var dur time.Duration
	var wg sync.WaitGroup

	byteSent := 0
	byteRead := 0

	interval := 40 * time.Millisecond //1/25 of a second
	cc := NewTimedContentConsumer("client1", interval, 2)
	var lastTime time.Time
	downstream := make(chan interface{}, 10)
	eventstream := make(chan ConsumerEventPtr, 10)
	cc.Downstream = downstream
	cc.EventDownstream = eventstream
	go cc.Run(&wg)

	go func(wg *sync.WaitGroup) {
		wg.Add(1)
		defer wg.Done()
		lastTime = time.Now()
		for downstream != nil && eventstream != nil {
			select {
			case data, ok := <-downstream:
				if !ok {
					downstream = nil
					continue
				}
				if data != nil {
					bytes := data.([]byte)
					timeNow := time.Now()
					diff := timeNow.Sub(lastTime)
					t.Logf("Read Bytes:%v Diff:%v", bytes, diff)
					if data != nil {
						byteRead += len(data.([]byte))
					}
					lastTime = timeNow
				}
			case err, ok := <-eventstream:
				if !ok {
					eventstream = nil
					continue
				}
				if err != nil {
					t.Logf("%v", err)
				}
			}

		}
	}(&wg)
	c := cc.Channel()
	data := []byte("ABCDEF")

	for i := 0; i < 5; i++ {
		dur = 2 * time.Second
		c <- NewTimedContent(dur, data)
		byteSent += len(data)
		t.Logf("Write Dur:%v Bytes:%v", dur, data)
		time.Sleep(40 * time.Millisecond)
	}
	t.Logf("Finished Writing...")
	time.Sleep(10 * time.Second)
	t.Logf("Finished Sleeping...")
	cc.CloseChannel()
	t.Logf("Finished Closing Channel...")
	wg.Wait()
	t.Logf("Finished Wait...")
	if cc.Channel() != nil {
		t.Errorf("Closed channel returning not null")
	}
	if byteRead != byteSent {
		t.Errorf("Content Read failed : Exp %v, Act:%v", byteSent, byteRead)
		return
	}
	t.Logf("Bytes Read : %v", byteRead)
}
