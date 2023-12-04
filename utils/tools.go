package utils

import "fmt"

func ValueSize(value []byte) int64 {
	return 0
}

type any = interface{}

func Print(a ...any) {
	fmt.Println(a)
}
