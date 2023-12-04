package utils

// TODO: 日志和报错文件信息

func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

func CondPanic(condtion bool, err error) {
	if condtion {
		Panic(err)
	}
}
