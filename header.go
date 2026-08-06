package gate

// 线路帧头，规格见 02「帧头」。
//
// 首字节高 4 位是标记位，与长度共享 16 bit：
//
//	m=0：2 字节头，size ∈ [0, 4095]（高 4 位在首字节低半，低 8 位在第二字节）
//	m=1：4 字节头，size = 高12 × 65536 + 低16，且必须 size >= 4096（规范编码）
//
// 大端序。m=1 && size < 4096 是不可达编码，保留作唯一扩展点。
const (
	flagM byte = 0x80 // more header：头是 4 字节而不是 2 字节
	flagZ byte = 0x40 // compressed：payload 是 zstd 压缩后的字节
	flagC byte = 0x20 // compound：payload 是若干条子消息拼接而成
	flagE byte = 0x10 // encrypted：payload 是密文

	flagAll = flagZ | flagC | flagE // parse 策略掩码管辖的三个标记位（m 是结构性标记，任何策略下都允许）

	minHeaderSize  = 2
	maxHeaderSize  = 4
	smallSizeLimit = 1 << 12 // 2 字节头可表达 size ∈ [0, smallSizeLimit)
	maxHeaderLen   = 1 << 28 // 长度字段可表达的编码总数（12 + 16 位）

	// maxMessageSize 是协议上限。Limits.MaxMessage 只能在它之内收紧。
	maxMessageSize = 32 << 20
)

// maxMessageSize 必须严格小于 header 能表达的上限，
// 否则长度高位会溢出进 m/z/c/e 标记位——长度和标记同时改错且两端都察觉不到。
// 这里的负常量转 uint 会直接编译失败。
const _ = uint(maxHeaderLen - 1 - maxMessageSize)

// headerSize 返回 size 的规范编码头长（2 或 4）。
func headerSize(size int) int {
	if size < smallSizeLimit {
		return minHeaderSize
	}
	return maxHeaderSize
}

// frameSize 是所有队列计量、预算扣减、分组累加的唯一口径：payload + header。
// 空消息在线路上占 2 字节，按 len 计量会让它不消耗任何配额（02「计量口径」）。
func frameSize(payload int) int {
	return payload + headerSize(payload)
}

// putHeader 在 dst 头部写入 size 的规范编码帧头并置入标记位，返回头长。
// dst 容量必须 >= maxHeaderSize。
//
// 越界 size 说明 gate 自身的准入检查漏了，这不是对端能触发的路径，直接 panic
// 好过写一个「长度和标记同时错、两端都察觉不到」的帧上线路。
func putHeader(dst []byte, size int, flags byte) int {
	if uint(size) >= maxHeaderLen {
		panic("gate: frame size overflows header")
	}
	if size < smallSizeLimit {
		dst[0] = flags | byte(size>>8)
		dst[1] = byte(size)
		return minHeaderSize
	}
	dst[0] = flagM | flags | byte(size>>24)
	dst[1] = byte(size >> 16)
	dst[2] = byte(size >> 8)
	dst[3] = byte(size)
	return maxHeaderSize
}
