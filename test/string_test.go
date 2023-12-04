package test

import (
	"bytes"
	"corekv/utils"
	"fmt"

	"testing"
)

func TestString(t *testing.T) {
	utils.Print(bytes.Compare([]byte(""), []byte("bcd")))

}

func TestPlus(t *testing.T) {
	for i := 0; i < iteration; i++ {
		fmt.Printf("utils.Randextract(10): %v\n", utils.Randextract(4))
	}
}
