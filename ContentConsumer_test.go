package contentconsumer

import (
	"sync"
	"testing"
	"time"
)

func TestDrainPositive(t *testing.T) {
	var dur time.Duration
	var remain time.Duration
	var err error
	ringLen := 5
	cc := NewContentConsumer("client1", ringLen)
	if cc.writerOwner.Len() != ringLen {
		t.Errorf("Ring length not matching Exp:%v Act:%v", ringLen, cc.writerOwner.Len())
		return
	}
	t.Logf("Ring of %v created", ringLen)
	cc.lastTimeEval = time.Now()
	for i := 0; i < 5; i++ {
		dur = 2 * time.Second
		err = cc.addchunk(NewChunkContent(dur, nil))
		if err != nil {
			t.Errorf("%s", err.Error())
			return
		}
		t.Logf("Added %v worth content, Tot: %v", dur, cc.bufferAvailPeriod)
	}
	for i := 0; i < 2; i++ {
		dur = 2 * time.Second
		err = cc.addchunk(NewChunkContent(dur, nil))
		if err != nil {
			t.Logf("%s", err.Error())
		} else {
			t.Logf("Added %v worth content, Tot: %v", dur, cc.bufferAvailPeriod)
			return
		}
	}
	dur = 7 * time.Second
	remain, err = cc.consumeNextChunk(dur)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	if remain <= 0 {
		t.Errorf("Remaining Exp:%v Act:%v", 1*time.Second, remain)
	}
	t.Logf("Got %v worth content. %v Ask, %v remain, %v Avail", dur-remain, dur, remain, cc.bufferAvailPeriod)
	dur = 2 * time.Second
	remain, err = cc.consumeNextChunk(dur)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	t.Logf("Got %v worth content. %v Ask, %v remain, %v Avail", dur-remain, dur, remain, cc.bufferAvailPeriod)
	dur = 3 * time.Second
	remain, err = cc.consumeNextChunk(dur)
	if err == nil {
		t.Errorf("ERROR : Got %v worth content", dur)
		return
	}
	t.Logf("Got %v worth content. %v Ask, %v remain, %v Avail", dur-remain, dur, remain, cc.bufferAvailPeriod)
	dur = 2 * time.Second
	remain, err = cc.consumeNextChunk(dur)
	if err == nil {
		t.Errorf("Got %v worth content. %v Ask, %v remain, %v Avail", dur-remain, dur, remain, cc.bufferAvailPeriod)
		return
	}
	t.Logf("GOOD: Found expected Error getting content : %s, %v Avail", err.Error(), cc.bufferAvailPeriod)
	dur = 2 * time.Second
	err = cc.addchunk(NewChunkContent(dur, nil))
	if err != nil {
		t.Logf("%s", err.Error())
	} else {
		t.Logf("Added %v worth content, Tot: %v", dur, cc.bufferAvailPeriod)
		return
	}
	remain, err = cc.consumeNextChunk(dur)
	if err == nil {
		t.Errorf("ERROR : Got %v worth content", dur)
		return
	}
	t.Logf("Got %v worth content. %v Ask, %v remain, %v Avail", dur-remain, dur, remain, cc.bufferAvailPeriod)

}

func TestFeed(t *testing.T) {
	var dur time.Duration
	var wg sync.WaitGroup
	ringLen := 5
	durRead := 0 * time.Second
	byteRead := 0
	cc := NewContentConsumer("client1", ringLen)
	if cc.writerOwner.Len() != ringLen {
		t.Errorf("Ring length not matching Exp:%v Act:%v", ringLen, cc.writerOwner.Len())
		return
	}
	cc.SetConsumptionInterval(40 * time.Millisecond) //1/25 of a second
	var lastTime time.Time
	lastTime = time.Now()
	cc.SetPostDataFunc(func(dur time.Duration, data []byte) {
		timeNow := time.Now()
		diff := timeNow.Sub(lastTime)
		t.Logf("Read Dur:%v Bytes:%v Diff:%v", dur, data, diff)
		durRead += dur
		if data != nil {
			byteRead += len(data)
		}
		lastTime = timeNow
	})
	t.Logf("Ring of %v created", ringLen)
	c := cc.GetChannel()
	data := []byte("ABCDEF")
	go cc.Run(&wg)
	for i := 0; i < 5; i++ {
		dur = 2 * time.Second
		c <- NewChunkContent(dur, data)
		t.Logf("Write Dur:%v Bytes:%v", dur, data)
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(10 * time.Second)
	cc.CloseChannel()
	wg.Wait()
	if durRead != 10*time.Second {
		t.Errorf("Content Read failed : Exp %v, Act:%v", 10*time.Second, durRead)
		return
	}
	t.Logf("Dur Read : %v, Bytes Read : %v", durRead, byteRead)
}
