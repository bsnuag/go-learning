package file

import (
	"fmt"
	"github.com/niubaoshu/gotiny"
	"golang.org/x/sys/unix"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var recSize int64 = 4000

func randReadPerf(fileName string, offsets []int64) {
	start1 := time.Now()
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	count := 0
	buf := make([]byte, recSize)
	for _, offset := range offsets {
		file.ReadAt(buf, offset)
		emp := Employee{}
		gotiny.Unmarshal(buf, &emp)
		if count == 10 {
			fmt.Println(emp)
		}
		if count == 100 {
			fmt.Println(emp)
		}
		if count == 1000 {
			fmt.Println(emp)
		}
		count++
	}
	file.Close()
	duration1 := time.Since(start1)
	fmt.Println("record reads- ", count)
	fmt.Println("total time to complete-", duration1)
}

func mmapReadPerf(fileName string, offsets []int64) {
	start1 := time.Now()
	file, err := os.Open(fileName)
	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fSize := stat.Size()
	filBuf, err := unix.Mmap(int(file.Fd()), 0, int(fSize), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}

	/*	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&filBuf[0])),
			uintptr(len(filBuf)), uintptr(unix.MADV_NORMAL|unix.MADV_RANDOM))
		if e1 != 0 {
			err = e1
		}
	*/
	start2 := time.Now()
	count := 0
	for _, offset := range offsets {
		bytes := filBuf[offset : offset+recSize]
		emp := Employee{}
		gotiny.Unmarshal(bytes, &emp)
		if count == 10 {
			fmt.Println(emp)
		}
		if count == 100 {
			fmt.Println(emp)
		}
		if count == 1000 {
			fmt.Println(emp)
		}
		count++
	}

	file.Close()
	duration1 := time.Since(start1)
	fmt.Println("total time to complete(excluding mmap and unmap)-", duration1)
	unix.Munmap(filBuf)
	duration2 := time.Since(start2)

	fmt.Println("record reads- ", count)
	fmt.Println("total time to complete(including mmap and unmap)-", duration2)
}

func loadDataToFile(file *os.File, nEmp int64) (map[int64]byte, error) {
	start := time.Now()

	name := "ABCDEFDSHSDHSOHSODHSIWQCHdbw ixbiqb sdibcibw xibqr xubvuasvqbaeksn, DC 3QWYEGFN83G2QIASBDXHVCERUQEWNakdxbue wkchaXDZUDAGVDUabjkjn,S XSiayk e	cwaSKAVDUA ECFKZSJCSADVQ Ukdaxuqvwufcv AUVDWVDVVVUWEF ibfwqvj BXDVhxa uvi wqxiaCD8GBWEIawudxuyewqudjahdsbv ubkhjiwkcheasbiyxguqefganiqgwfeuygnciyqwfguaxegueqwegafuxygugxquwygacsibfgquxgaueguueadgsxugucgqurgaxugqwgugxqdvaugudacgusugavugaudgvugadcgdugeuavdgsuxqxasugqwfeacubiasucygabfgckwascfgcuZYGuxasgczbbds_Name_"
	city := "ABCDEFDSHSDHSOHSODHSIWQCHdbw ixbiqb sdibcibw xibqr xubvuasvqbaeksn, DC 3QWYEGFN83G2QIASBDXHVCERUQEWNakdxbue wkchaXDZUDAGVDUabjkjn,S XSiayk e	cwaSKAVDUA ECFKZSJCSADVQ Ukdaxuqvwufcv AUVDWVDVVVUWEF ibfwqvj BXDVhxa uvi wqxiaCD8GBWEIawudxuyewqudjahdsbv ubkhjiwkcheasbiyxguqefganiqgwfeuygnciyqwfguaxegueqwegafuxygugxquwygacsibfgquxgaueguueadgsxugucgqurgaxugqwgugxqdvaugudacgusugavugaudgvugadcgdugeuavdgsuxqxasugqwfeacubiasucygabfgckwascfgcuZYGuxasgczbbds_City_"
	country := "ABCDEFDSHSDHSOHSODHSIWQCHdbw ixbiqb sdibcibw xibqr xubvuasvqbaeksn, DC 3QWYEGFN83G2QIASBDXHVCERUQEWNakdxbue wkchaXDZUDAGVDUabjkjn,S XSiayk e	cwaSKAVDUA ECFKZSJCSADVQ Ukdaxuqvwufcv AUVDWVDVVVUWEF ibfwqvj BXDVhxa uvi wqxiaCD8GBWEIawudxuyewqudjahdsbv ubkhjiwkcheasbiyxguqefganiqgwfeuygnciyqwfguaxegueqwegafuxygugxquwygacsibfgquxgaueguueadgsxugucgqurgaxugqwgugxqdvaugudacgusugavugaudgvugadcgdugeuavdgsuxqxasugqwfeacubiasucygabfgckwascfgcuZYGuxasgczbbds_Country_"

	var offset int64 = 0
	offsets := make(map[int64]byte)  //just to have random iterator
	byteBuf := make([]byte, recSize) //at least recSizebytes
	var i int64 = 1;
	for ; i < nEmp; i++ {
		st := string(i)
		emp := Employee{
			id:      i,
			name:    name + st,
			city:    city + st,
			country: country + st,
		}
		marshal := gotiny.Marshal(&emp)
		copy(byteBuf[0:recSize], marshal)
		n, err := file.Write(byteBuf)
		if err != nil {
			panic(err)
		}
		offsets[offset] = 1
		offset += int64(n)
	}
	err := file.Sync()
	if err != nil {
		return nil, err
	}

	err = file.Close()
	if err != nil {
		return nil, err
	}
	duration1 := time.Since(start)
	fmt.Println("total time to load data-", duration1)
	return offsets, nil
}

func createTempFile() (*os.File, error) {
	dir, err := ioutil.TempDir("", "data")
	if err != nil {
		log.Fatal(err)
	}
	file, err := ioutil.TempFile(dir, "ReadTest.*.txt")
	if err != nil {
		return nil, err
	} else {
		fmt.Println("Created file: ", file.Name())
	}
	return file, nil
}

func deleteTempFile(file string) {
	os.Remove(file)
	//os.RemoveAll(dir)
}

type Employee struct {
	id      int64
	name    string
	city    string
	country string
}
