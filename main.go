package main

import "fmt"

func main() {
	table := [...]byte{0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01}
	var x uint8 = 10
	fmt.Println(x & table[5])
}
