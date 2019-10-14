package file

import (
	"sort"
	"testing"
)

func TestMmapSeqReadPerf(t *testing.T) {
	file, _ := createTempFile()
	offsets, _ := loadDataToFile(file, 2500000*2)
	offsetKeys := sortedKeys(offsets)
	mmapReadPerf(file.Name(), offsetKeys)
	deleteTempFile(file.Name())
}

func TestMmapReadPerf(t *testing.T) {
	file, _ := createTempFile()
	offsets, _ := loadDataToFile(file, 2500000*2)
	offsetKeys := keys(offsets)
	mmapReadPerf(file.Name(), offsetKeys)
	deleteTempFile(file.Name())
}

func TestRandReadPerf(t *testing.T) {
	file, _ := createTempFile()
	offsets, _ := loadDataToFile(file, 2500000*2)
	offsetKeys := keys(offsets)
	randReadPerf(file.Name(), offsetKeys)
	deleteTempFile(file.Name())
}

func TestSeqReadPerf(t *testing.T) {
	file, _ := createTempFile()
	offsets, _ := loadDataToFile(file, 2500000*2)
	sortedKeys := sortedKeys(offsets)
	randReadPerf(file.Name(), sortedKeys)
	deleteTempFile(file.Name())
}

func keys(offsets map[int64]byte) []int64 {
	offsetsArr := make([]int64, len(offsets))
	var index int64 = 0
	for offset := range offsets {
		offsetsArr[index] = offset
		index += 1
	}
	return offsetsArr
}

func sortedKeys(offsets map[int64]byte) []int64 {
	offsetsArr := keys(offsets)
	sort.Slice(offsetsArr, func(i, j int) bool {
		return offsetsArr[i] < offsetsArr[j]
	})
	return offsetsArr
}
