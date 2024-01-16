package utils

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
)

// TODO: 日志和报错文件信息

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyEmpty    = errors.New("key cannot be empty")

	ErrCheckSumMismatch = errors.New("checksum mismatch")
	ErrSSTIndex         = errors.New("read index fail, offset is nil")
	ErrInteger          = errors.New("interger overflow")
	ErrBuilderAppend    = errors.New("tablebuilder append data")
	ErrBlockOutIndex    = errors.New("block out of index")
	ErrChecksumLen      = errors.New("invalid checksum length. Either the data is corrupted or the table options are incorrectly set")

	ErrTruncate = errors.New("Do Truncate")
	ErrStop     = errors.New("Stop")

	ErrReWriteFailure = errors.New("rewrite failure")
	ErrBadMagic       = errors.New("bad magic")
	ErrBadChecksum    = errors.New("bad checksum")
)

// err非空panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// condition true中断err
func CondPanic(condtion bool, err error) {
	if condtion {
		Panic(err)
	}
}

func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	file = filepath.Base(file)

	return file + ":" + strconv.Itoa(line)
}
