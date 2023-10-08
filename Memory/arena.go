package memory

/*
 * arena只保证数据追加功能，更新数据直接到内存池后加入，达到阈值直接固化为sst再去释放空间
 */

type arena struct{
	n int
	buffer []byte
}
