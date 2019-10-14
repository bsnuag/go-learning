package concurrency

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"sync"
)

/*
	Test where we create n (Million) go routine to write sample data to Directory
*/

type ChannelRec struct {
	key    string
	value  string
}

type StudentDirectory struct {
	dir  map[string]string
	lock sync.Mutex
}

var channelRecs chan *ChannelRec
var studentDirMap map[int]*StudentDirectory
var dirSize int = 4
var wg sync.WaitGroup


func prepareStudentDirMap(dirSize int) {
	studentDirMap = make(map[int]*StudentDirectory)

	for i := 0; i < dirSize; i++ {
		dir := &StudentDirectory{
			dir: make(map[string]string),
		}
		studentDirMap[i] = dir
	}
}


func PrepareWriteWithoutChannel() {
	prepareStudentDirMap(dirSize)
	wg = sync.WaitGroup{}
}

func WriteWithoutChannel(key string, value string) {
	wg.Add(1)
	go func() {
		directory := studentDirMap[int(murmur3.Sum32([]byte(key)))%dirSize]
		directory.lock.Lock()
		directory.dir[key] = value
		directory.lock.Unlock()
		wg.Done()
	}()
}

func CloseWriteWithoutChannel() {
	wg.Wait()
}



func PrepareWriteWithChannel() {
	prepareStudentDirMap(dirSize)
	channelRecs = make(chan *ChannelRec, 90000000)
	wg = sync.WaitGroup{}
	wg.Add(dirSize)

	for j := 0; j < dirSize; j++ {
		go writeToDir(channelRecs, &wg)
	}
}

func WriteWithChannel(key string, value string) {
	cRec := &ChannelRec{
		key:    key,
		value:  value,
	}
	channelRecs <- cRec
}

func writeToDir(recs chan *ChannelRec, wg *sync.WaitGroup) {
	for {
		rec, valid := <-recs
		if !valid {
			break
		} else {
			directory := studentDirMap[int(murmur3.Sum32([]byte(rec.key)))%dirSize]
			directory.lock.Lock()
			directory.dir[rec.key] = rec.value
			directory.lock.Unlock()
		}
	}
	wg.Done()
}

func CloseWriteWithChannel() {
	close(channelRecs)
	wg.Wait()
}

func PrintStudentDirStat() {
	for _, v := range studentDirMap {
		fmt.Println("Directory Size: ", len(v.dir))
	}
}
