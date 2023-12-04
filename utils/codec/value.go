package codec

type ValueStruct struct {
}

func NewValueStruct(entry *Entry) *ValueStruct {
	return &ValueStruct{}
}

func IsValuePtr(entry *Entry) bool {
	return false
}

func ValueStructDecode(data []byte) *ValueStruct {
	return nil
}
