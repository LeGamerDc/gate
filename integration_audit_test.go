package gate

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"net/http"

	"github.com/gobwas/ws"
)

// 握手期间入队的消息必须在握手完成后自动发出。
//
// sender 在连接尚不可写（WebSocket 握手中）时会把消息留在队列里并复位 triggered，
// 此时没有任何 Wake 在路上。如果握手完成后没人主动踢一脚，而业务又不再调用 Send
// （这个 handler 故意不实现 ReadyHandler，也不回声任何东西），这批消息就会永远滞留。
func TestWebSocketFlushesMessagesQueuedDuringHandshake(t *testing.T) {
	const want = "queued before the handshake finished"

	srv := startTestWebSocketServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			// Build 发生在 OnOpen，此刻 WS 握手还没完成，连接还不可写。
			if conn.writable() {
				t.Error("connection must not be writable at Build time on websocket transport")
			}
			if err := conn.SendNoEncrypt([]byte(want)); err != nil {
				t.Errorf("send during handshake: %v", err)
			}
			return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {
				t.Error("handler must not receive anything in this test")
			}}
		}},
	})

	conn := dialTestWebSocketConn(t, srv.Addr(), "/")
	// 客户端一个字节都不发，消息只能靠握手完成时的主动 flush 送达。
	got := waitWebSocketMessages(t, conn, nil, 1)
	if string(got[0]) != want {
		t.Fatalf("got %q, want %q", got[0], want)
	}
}

// TCP 侧对照：Build 阶段连接已经可写，消息应当照常送达。
func TestTCPConnIsWritableAtBuildTime(t *testing.T) {
	const want = "sent from Build"

	srv := startTestServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			if !conn.writable() {
				t.Error("tcp connection should be writable at Build time")
			}
			if err := conn.SendNoEncrypt([]byte(want)); err != nil {
				t.Errorf("send from Build: %v", err)
			}
			return &testConnHandler{conn: conn, handle: func(*Conn, []byte) {}}
		}},
	})

	_, handler := startTestClient(t, srv.Addr(), nil)
	if got := waitMessage(t, handler.msgCh); string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 握手请求与紧随其后的数据帧在同一个 TCP 段里到达时，数据帧不能被丢掉。
//
// 之前 upgrade() 用 oldLen-tmpReader.Len() 反推消耗量，而 ws.Upgrader 内部的
// 池化 bufio.Reader 会预读，于是这个差值大于握手本身的长度，后面的数据帧字节
// 被一并丢弃。RFC 要求客户端等 101 之后再发帧，所以合规实现碰不到，但流水线
// 发送的客户端会静默丢消息。
func TestWebSocketHandshakePipelinedFrameIsNotDiscarded(t *testing.T) {
	const want = "pipelined right after the handshake"

	received := make(chan string, 4)
	srv := startTestWebSocketServer(t, &Config{
		CHB: testHandlerBuilder{build: func(conn *Conn) ConnHandler {
			return &testConnHandler{conn: conn, handle: func(_ *Conn, raw []byte) {
				received <- string(raw)
			}}
		}},
	})

	raw, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	// 握手请求 + 一个已掩码的 WS 二进制帧，一次 Write 送出去。
	var wire bytes.Buffer
	fmt.Fprintf(&wire,
		"GET / HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
		srv.Addr())

	gateFrame := mkFrame([]byte(want), 0)
	wsFrame := ws.MaskFrame(ws.NewBinaryFrame(gateFrame))
	if err := ws.WriteFrame(&wire, wsFrame); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write(wire.Bytes()); err != nil {
		t.Fatal(err)
	}

	// 读掉 101 响应。
	if err := raw.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(raw), nil)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake status = %d, want 101", resp.StatusCode)
	}

	select {
	case got := <-received:
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the frame pipelined with the handshake was discarded")
	}
}
