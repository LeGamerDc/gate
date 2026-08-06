package gate

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// WebSocketOptions 配置 WebSocket 传输（Options.WebSocket 非 nil ⇒ WS 模式）。
type WebSocketOptions struct {
	Path              string                 // 非空则只接受该路径的升级请求
	MaxHandshakeBytes int                    // 单个握手请求字节数上限；0 用默认 16KB
	OnUpgrade         func(*Handshake) error // 写出 101 之前调用；返回 error 拒绝升级
}

const (
	defaultMaxHandshakeBytes = 16 << 10
	maxHandshakeHeaderLines  = 64
	wsAcceptGUID             = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

// upgradeRejection 携带真正的 HTTP 状态码——OnUpgrade 在 101 之前，
// 可以给对端一个可解释的拒绝而不是莫名断线（W-D4/W-D8）。
type upgradeRejection struct {
	status int
	reason string
}

func (e *upgradeRejection) Error() string {
	return fmt.Sprintf("gate: upgrade rejected: %d %s", e.status, e.reason)
}

// RejectUpgrade 构造 OnUpgrade 的拒绝返回值。不泄漏底层 WebSocket 库的类型。
func RejectUpgrade(status int, reason string) error {
	return &upgradeRejection{status: status, reason: reason}
}

var (
	errWSHandshakeTooLarge = errors.New("gate/ws: handshake exceeds MaxHandshakeBytes")
	errWSHandshakeHeaders  = errors.New("gate/ws: too many header lines") // 拒绝，不截断（W5/W-D5）
	errWSBadUpgrade        = errors.New("gate/ws: not a websocket upgrade")
)

var crlfcrlf = []byte("\r\n\r\n")

// parseUpgrade 解析恰好一个完整请求（reqBytes 已按 \r\n\r\n 切好边界——
// 绝不把整个缓冲扔给解析器，防止带缓冲的 reader 预读吃掉握手后的帧字节）。
func parseUpgrade(reqBytes []byte, opts *WebSocketOptions) (*Handshake, string, error) {
	// 头条数上限：拒绝而不是截断——静默截断会让鉴权基于不完整的头做判断。
	if bytes.Count(reqBytes, []byte("\r\n"))-2 > maxHandshakeHeaderLines {
		return nil, "", errWSHandshakeHeaders
	}
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(reqBytes)))
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", errWSBadUpgrade, err)
	}
	if req.Method != http.MethodGet {
		return nil, "", fmt.Errorf("%w: method %s", errWSBadUpgrade, req.Method)
	}
	if opts.Path != "" && req.URL.Path != opts.Path {
		return nil, "", &upgradeRejection{status: http.StatusNotFound, reason: "not found"}
	}
	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") ||
		!headerContainsToken(req.Header.Get("Connection"), "upgrade") {
		return nil, "", fmt.Errorf("%w: missing upgrade headers", errWSBadUpgrade)
	}
	if req.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, "", fmt.Errorf("%w: unsupported version", errWSBadUpgrade)
	}
	key := req.Header.Get("Sec-WebSocket-Key")
	// RFC 6455 §4.1：必须是 16 字节随机数的 base64。只查非空会接受无效握手。
	if k, err := base64.StdEncoding.DecodeString(key); err != nil || len(k) != 16 {
		return nil, "", fmt.Errorf("%w: bad key", errWSBadUpgrade)
	}
	return &Handshake{URI: req.RequestURI, Header: req.Header}, key, nil
}

func headerContainsToken(v, token string) bool {
	for part := range strings.SplitSeq(v, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func wsAcceptKey(key string) string {
	h := sha1.Sum([]byte(key + wsAcceptGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// build101 生成升级响应。绝不协商 permessage-deflate / 子协议 / 任何扩展（W11）。
func build101(key string) []byte {
	return fmt.Appendf(nil,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
		wsAcceptKey(key))
}

func buildHTTPError(status int, reason string) []byte {
	text := http.StatusText(status)
	if text == "" {
		text = "Error"
	}
	return fmt.Appendf(nil,
		"HTTP/1.1 %d %s\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		status, text, len(reason), reason)
}

// wsReject 排入 HTTP 错误响应并关闭（06 迁移表：Handshaking + 拒绝 →
// Draining，出站链里放 HTTP 错误响应；连接未 Open 过，不会调 OnClose）。
func (l *loop) wsReject(c *connCore, status int, reason string, cause error) {
	_ = c.out.sendRaw(buildHTTPError(status, reason), false)
	l.closeLocal(c, cause)
	l.flushBatchEnd(c)
}

// wsHsReadable 处理 Handshaking 状态的读事件：自己定位请求边界。
// done 在 101 排入出站链之后、OnOpen 之前回调（server 在此绑定壳）。
func (l *loop) wsHsReadable(c *connCore, opts *WebSocketOptions, done func(hs *Handshake)) {
	maxHS := opts.MaxHandshakeBytes
	if maxHS <= 0 {
		maxHS = defaultMaxHandshakeBytes
	}
	for range readBudgetCalls {
		// PROXY 阶段的剩余字节可能已含完整升级请求：先试 carry 再读。
		if !bytes.Contains(c.in.carry, crlfcrlf) {
			n, err := l.io.read(c.fd, l.rbuf)
			if !l.readOK(c, n, err) {
				return
			}
			c.in.appendCarry(l.rbuf[:n]) // 握手期的上界是 maxHS，见下面的边界检查
		}
		data := c.in.carry
		idx := bytes.Index(data, crlfcrlf)
		if idx < 0 {
			if len(data) > maxHS {
				l.wsReject(c, http.StatusRequestHeaderFieldsTooLarge, "handshake too large",
					wrapProtocol(errWSHandshakeTooLarge))
				return
			}
			continue // 等更多数据
		}
		reqEnd := idx + 4
		// 一次大 read 可能直接送来一个超限但完整的请求：找到边界后仍要检查（W5）。
		if reqEnd > maxHS {
			l.wsReject(c, http.StatusRequestHeaderFieldsTooLarge, "handshake too large",
				wrapProtocol(errWSHandshakeTooLarge))
			return
		}

		hs, key, perr := parseUpgrade(data[:reqEnd], opts)
		if perr != nil {
			var rej *upgradeRejection
			if errors.As(perr, &rej) {
				l.wsReject(c, rej.status, rej.reason, perr)
			} else {
				l.wsReject(c, http.StatusBadRequest, "bad websocket handshake", wrapProtocol(perr))
			}
			return
		}
		if opts.OnUpgrade != nil {
			var uerr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						uerr = fmt.Errorf("%w: OnUpgrade: %v", ErrHandlerPanic, r)
					}
				}()
				uerr = opts.OnUpgrade(hs)
			}()
			if uerr != nil {
				var rej *upgradeRejection
				switch {
				case errors.As(uerr, &rej):
					l.wsReject(c, rej.status, rej.reason, uerr)
				case errors.Is(uerr, ErrHandlerPanic):
					// 01 的回调表：OnUpgrade panic 按 500 拒绝。
					l.wsReject(c, http.StatusInternalServerError, "internal error", uerr)
				default:
					// 01：返回任意非 nil error 即拒绝握手（默认 401）。
					l.wsReject(c, http.StatusUnauthorized, "unauthorized", uerr)
				}
				return
			}
		}

		// 接受：101 完整排入出站链 → Open → OnOpen → 处理剩余入站字节（W3）。
		// 握手期的解析状态（carry、req、闭包）在本函数返回时全部释放（W9）。
		_ = c.out.sendRaw(build101(key), false)
		carry := c.in.carry
		c.in.carry = nil
		c.in.syncPending(l.stats)
		leftover := carry[reqEnd:]

		enableWS(c)
		if done != nil {
			done(hs)
		}
		l.openConn(c)
		if c.state == stateOpen && len(leftover) > 0 {
			// 抢跑的客户端（不等 101 就发帧）：剩余字节直接进帧层。
			l.wsFeed(c, leftover)
		}
		poolPut(carry)
		l.flushBatchEnd(c)
		return
	}
	l.truncated = true
}
