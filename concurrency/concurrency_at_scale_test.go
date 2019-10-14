package concurrency

import (
	"fmt"
	"testing"
)

func BenchmarkWriteWithChannel(b *testing.B) {
	PrepareWriteWithChannel()
	for i:=0;i<b.N;i++{
		key:=fmt.Sprintf("Key:%d",i)
		value:=fmt.Sprintf("Value:%d",i)
		WriteWithChannel(key, value)
	}
	CloseWriteWithChannel()
}

func BenchmarkWriteWithoutChannel(b *testing.B) {
	PrepareWriteWithoutChannel()
	for i:=0;i<b.N;i++{
		key:=fmt.Sprintf("Key:%d",i)
		value:=fmt.Sprintf("Value:%d",i)
		WriteWithoutChannel(key, value)
	}
	CloseWriteWithoutChannel()
}
