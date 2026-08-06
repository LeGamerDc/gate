package gate

import "testing"

func TestHeaderSizeAndFrameSize(t *testing.T) {
	cases := []struct {
		payload, hdr int
	}{
		{0, 2}, {1, 2}, {4095, 2}, {4096, 4}, {65535, 4}, {65536, 4}, {maxMessageSize, 4},
	}
	for _, tc := range cases {
		if got := headerSize(tc.payload); got != tc.hdr {
			t.Fatalf("headerSize(%d) = %d, want %d", tc.payload, got, tc.hdr)
		}
		if got := frameSize(tc.payload); got != tc.payload+tc.hdr {
			t.Fatalf("frameSize(%d) = %d", tc.payload, got)
		}
	}
}

func TestPutHeaderBigEndianLayout(t *testing.T) {
	var h [maxHeaderSize]byte

	// size=4095, flags=z|e：0100 1111 1111 1111 → 0x5F 0xFF
	if n := putHeader(h[:], 4095, flagZ|flagE); n != 2 || h[0] != flagZ|flagE|0x0F || h[1] != 0xFF {
		t.Fatalf("n=%d h=%x", n, h[:n])
	}
	// size=4096：m 置位，高12=0，低16=0x1000
	if n := putHeader(h[:], 4096, 0); n != 4 || h[0] != flagM || h[1] != 0x00 || h[2] != 0x10 || h[3] != 0x00 {
		t.Fatalf("n=%d h=%x", n, h[:n])
	}
	// size = 高12×65536 + 低16 的拼法：0x0ABCDEF = 高12 0x0AB，低16 0xCDEF
	if n := putHeader(h[:], 0x0ABCDEF, flagC); n != 4 ||
		h[0] != flagM|flagC || h[1] != 0xAB || h[2] != 0xCD || h[3] != 0xEF {
		t.Fatalf("n=%d h=%x", n, h[:n])
	}
}

// 长度溢出宁可 panic 也不写错帧上线路（02「长度字段溢出进标记位」）。
func TestPutHeaderOverflowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("putHeader(2^28) 未 panic")
		}
	}()
	var h [maxHeaderSize]byte
	putHeader(h[:], maxHeaderLen, 0)
}
