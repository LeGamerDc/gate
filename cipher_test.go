package gate

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
)

// xorCipher：Overhead() == 0 的流式形状。忽略 aad（01 写明这是实现自己的取舍，
// 代价是得不到帧头认证），支持 dst = src[:0] 的原地路径。
type xorCipher struct {
	key   byte
	sealN uint64
	openN uint64
}

func (x *xorCipher) Overhead() int { return 0 }

func (x *xorCipher) Seal(dst, plaintext, _ []byte) []byte {
	k := x.key ^ byte(x.sealN)
	x.sealN++
	for _, b := range plaintext {
		dst = append(dst, b^k)
	}
	return dst
}

func (x *xorCipher) Open(dst, ciphertext, _ []byte) ([]byte, error) {
	k := x.key ^ byte(x.openN)
	x.openN++
	for _, b := range ciphertext {
		dst = append(dst, b^k)
	}
	return dst, nil
}

// gcmCipher：真 AEAD（AES-128-GCM，Overhead() == 16），帧头 aad 进认证范围。
// 收发方向各自独立的计数器 nonce（01「收发方向必须分开」）：nonce 首字节是
// 方向标记，两个实例配对时 seal/open 方向交叉（A 的 seal 方向 = B 的 open 方向）。
type gcmCipher struct {
	aead             cipher.AEAD
	sealDir, openDir byte
	sealN, openN     uint64
}

func newGCM(key [16]byte, sealDir, openDir byte) *gcmCipher {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return &gcmCipher{aead: aead, sealDir: sealDir, openDir: openDir}
}

func (g *gcmCipher) Overhead() int { return g.aead.Overhead() }

func gcmNonce(n uint64, dir byte) [12]byte {
	var nonce [12]byte
	nonce[0] = dir
	binary.BigEndian.PutUint64(nonce[4:], n)
	return nonce
}

func (g *gcmCipher) Seal(dst, plaintext, aad []byte) []byte {
	nonce := gcmNonce(g.sealN, g.sealDir)
	g.sealN++
	return g.aead.Seal(dst, nonce[:], plaintext, aad)
}

func (g *gcmCipher) Open(dst, ciphertext, aad []byte) ([]byte, error) {
	nonce := gcmNonce(g.openN, g.openDir)
	g.openN++
	return g.aead.Open(dst, nonce[:], ciphertext, aad)
}

// pairGCM 返回 (发送端, 接收端)：接收端 Open 的 nonce 序列对上发送端 Seal 的。
func pairGCM(key [16]byte) (*gcmCipher, *gcmCipher) {
	return newGCM(key, 'd', 'u'), newGCM(key, 'u', 'd')
}
