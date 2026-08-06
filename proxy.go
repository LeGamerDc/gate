package gate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// PROXY protocol（01「真实来源地址」）：挂四层 LB 后面时在第一个 gate 帧
// 之前解出真实来源。v1（文本）与 v2（二进制）都支持。

// ProxyMode 控制是否解析 PROXY 头。
type ProxyMode uint8

const (
	ProxyOff      ProxyMode = iota // 默认：不解析
	ProxyOptional                  // 有就解析，没有就用 socket 地址（仅灰度切换用）
	ProxyRequired                  // 必须有，否则拒绝连接。挂 LB 后面用这个
)

var (
	errProxyMalformed = errors.New("gate: malformed PROXY header")
	errProxyRequired  = errors.New("gate: PROXY header required")
)

var (
	proxyV1Sig = []byte("PROXY ")
	proxyV2Sig = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
)

const (
	proxyV1MaxLen = 107 // 规范定义的 v1 行最大长度
	proxyV2MinLen = 16
	// proxyMaxHeaderLen 是握手期累计字节的宽松上界：v2 的长度字段是 16 位，
	// TLV 区合法地可以到 64KB。收得下就行，真正的定界由格式自己负责。
	proxyMaxHeaderLen = proxyV2MinLen + 1<<16
)

// parseProxy 尝试从 data 头部解出一个 PROXY 头。
//
//	n > 0            ⇒ 解析成功，消费 n 字节；src 是真实来源（LOCAL/UNKNOWN 时无效）
//	n == 0, err nil  ⇒ 数据不足，等更多字节
//	err != nil       ⇒ 头非法（或 required 但对不上签名）
func parseProxy(data []byte, required bool) (src netip.AddrPort, n int, err error) {
	if len(data) == 0 {
		return netip.AddrPort{}, 0, nil
	}
	// 「首字节撞上签名」不等于「这是个坏 PROXY 头」：一条 m=0、长度在
	// [0x0D00, 0x0DFF] 的合法 gate 帧首字节就是 0x0D。Optional 模式下
	// 签名对不上必须回退成普通字节流，否则合法客户端会被当成畸形头断开。
	bad := func() (netip.AddrPort, int, error) {
		if required {
			return netip.AddrPort{}, 0, errProxyMalformed
		}
		return netip.AddrPort{}, -1, nil
	}
	if data[0] == proxyV2Sig[0] {
		if len(data) < len(proxyV2Sig) {
			if bytes.HasPrefix(proxyV2Sig, data) {
				return netip.AddrPort{}, 0, nil // 还看不出来，等更多字节
			}
			return bad()
		}
		if !bytes.HasPrefix(data, proxyV2Sig) {
			return bad()
		}
		return parseProxyV2(data)
	}
	if data[0] == 'P' {
		probe := min(len(data), len(proxyV1Sig))
		if !bytes.Equal(data[:probe], proxyV1Sig[:probe]) {
			return bad()
		}
		if len(data) < len(proxyV1Sig) {
			return netip.AddrPort{}, 0, nil
		}
		return parseProxyV1(data)
	}
	if required {
		return netip.AddrPort{}, 0, errProxyRequired
	}
	return netip.AddrPort{}, -1, nil // 不是 PROXY 头：Optional 模式下当普通字节
}

func parseProxyV1(data []byte) (netip.AddrPort, int, error) {
	idx := bytes.Index(data, []byte("\r\n"))
	if idx < 0 {
		if len(data) > proxyV1MaxLen {
			return netip.AddrPort{}, 0, errProxyMalformed
		}
		return netip.AddrPort{}, 0, nil // 等更多数据
	}
	line := string(data[:idx])
	n := idx + 2
	if n > proxyV1MaxLen+2 {
		return netip.AddrPort{}, 0, errProxyMalformed
	}
	parts := strings.Split(line, " ")
	// "PROXY UNKNOWN..."：合法但不携带地址。
	if len(parts) >= 2 && parts[1] == "UNKNOWN" {
		return netip.AddrPort{}, n, nil
	}
	if len(parts) != 6 || parts[0] != "PROXY" || (parts[1] != "TCP4" && parts[1] != "TCP6") {
		return netip.AddrPort{}, 0, errProxyMalformed
	}
	addr, err := netip.ParseAddr(parts[2])
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("%w: %w", errProxyMalformed, err)
	}
	port, err := strconv.ParseUint(parts[4], 10, 16)
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("%w: %w", errProxyMalformed, err)
	}
	return netip.AddrPortFrom(addr, uint16(port)), n, nil
}

func parseProxyV2(data []byte) (netip.AddrPort, int, error) {
	if len(data) < proxyV2MinLen {
		return netip.AddrPort{}, 0, nil
	}
	verCmd := data[12]
	if verCmd>>4 != 0x2 {
		return netip.AddrPort{}, 0, errProxyMalformed
	}
	switch verCmd & 0x0F {
	case 0x0, 0x1: // LOCAL / PROXY
	default: // 未定义的 command：规范要求拒绝，不能当 PROXY 处理
		return netip.AddrPort{}, 0, errProxyMalformed
	}
	fam := data[13]
	alen := int(binary.BigEndian.Uint16(data[14:16]))
	total := proxyV2MinLen + alen
	if len(data) < total {
		return netip.AddrPort{}, 0, nil
	}
	if verCmd&0x0F == 0x0 { // LOCAL：健康检查等，不带地址语义
		return netip.AddrPort{}, total, nil
	}
	// 低四位是 transport protocol：TCP listener 上只接受 STREAM（或 UNSPEC）。
	if p := fam & 0x0F; p != 0x0 && p != 0x1 {
		return netip.AddrPort{}, 0, errProxyMalformed
	}
	body := data[16:total]
	switch fam >> 4 {
	case 0x1: // AF_INET
		if alen < 12 {
			return netip.AddrPort{}, 0, errProxyMalformed
		}
		addr := netip.AddrFrom4([4]byte(body[0:4]))
		port := binary.BigEndian.Uint16(body[8:10])
		return netip.AddrPortFrom(addr, port), total, nil
	case 0x2: // AF_INET6
		if alen < 36 {
			return netip.AddrPort{}, 0, errProxyMalformed
		}
		addr := netip.AddrFrom16([16]byte(body[0:16]))
		port := binary.BigEndian.Uint16(body[32:34])
		return netip.AddrPortFrom(addr, port), total, nil
	default: // AF_UNSPEC / AF_UNIX：接受但不取地址
		return netip.AddrPort{}, total, nil
	}
}
