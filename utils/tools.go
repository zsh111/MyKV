package utils

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
)

func ValueSize(value []byte) int64 {
	return 0
}

type any = interface{}

func Print(a ...any) {
	fmt.Println(a)
}

func AssertTrue(b bool) {
	// 答应函数调用栈的错误信息
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
