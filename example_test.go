//go:build darwin || linux

package gate_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/LeGamerDc/gate"
)

type player struct {
	id   uint64
	room string
}

type echoHandler struct{}

func (echoHandler) OnOpen(c *gate.Conn[*player]) (*player, error) {
	return &player{id: c.ID()}, nil
}

func (echoHandler) OnMessage(c *gate.Conn[*player], msg []byte) error {
	// msg 只在本次调用内有效；传给 Send 是安全的（它立即复制）。
	return c.Send(msg)
}

func (echoHandler) OnClose(c *gate.Conn[*player], reason error) {
	switch {
	case errors.Is(reason, gate.ErrIdleTimeout):
		// 空闲回收
	case errors.Is(reason, gate.ErrBackpressure):
		// 慢客户端：这个数一涨就该去看下行带宽了
	case errors.Is(reason, gate.ErrProtocol):
		// 对端实现或意图有问题
	}
	// c.State 在 OnClose 期间仍然有效，注册表要在这里删干净。
	_ = c.State.id
}

// 最小可用的 TCP 网关。
func Example() {
	err := gate.Run(context.Background(), gate.Options[*player]{
		Addr:     "127.0.0.1:0",
		Handler:  echoHandler{},
		Outbound: gate.DefaultOutbound(),
	})
	if err != nil {
		log.Print(err)
	}
}

// WebSocket 模式：鉴权放在 OnUpgrade（101 之前），它能返回真正的 HTTP 状态码。
func ExampleWebSocketOptions() {
	opts := gate.Options[*player]{
		Addr:    "127.0.0.1:0",
		Handler: echoHandler{},
		WebSocket: &gate.WebSocketOptions{
			Path: "/game",
			OnUpgrade: func(h *gate.Handshake) error {
				if h.Header.Get("Authorization") == "" {
					return gate.RejectUpgrade(http.StatusUnauthorized, "unauthorized")
				}
				return nil
			},
		},
		Limits: gate.Limits{
			MaxConns:         100_000,
			Idle:             90 * time.Second,
			HandshakeTimeout: 5 * time.Second,
		},
	}
	srv, err := gate.Listen(opts)
	if err != nil {
		log.Print(err)
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	_ = srv.Addr()
}

// 广播：编码一次、分发 N 次。
func ExampleServer_NewFrame() {
	srv, err := gate.Listen(gate.Options[*player]{
		Addr:    "127.0.0.1:0",
		Handler: echoHandler{},
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	f, err := srv.NewFrame([]byte("room closing in 10s"))
	if err != nil {
		log.Print(err)
		return
	}
	var room []*gate.Conn[*player]
	for _, c := range room {
		_ = c.SendFrame(f) // 不复制、不改写；不需要任何释放动作
	}
}

// 零复制发送：业务直接把序列化结果写进 gate 的出站缓冲。
func ExampleConn_SendFunc() {
	var c *gate.Conn[*player]
	payload := []byte("serialized-message")

	err := c.SendFunc(len(payload), func(b []byte) (int, error) {
		return copy(b, payload), nil // 实际用法是 m.MarshalToSizedBufferVT(b)
	})
	if errors.Is(err, gate.ErrSendQueueFull) {
		fmt.Println("积压达到准入上限：消息从未入队，帧流没有洞")
	}
}

// 阻塞任务：f 跑在别的 goroutine 上，但仍属该连接的串行域
// （期间不投递新消息、不执行 Post）。
func ExampleConn_AsyncDo() {
	var c *gate.Conn[*player]

	// 只能在事件循环线程上调用：回调与 Post 的函数体。
	// **不能**在另一个 AsyncDo 的函数体里调——那跑在别的 goroutine 上，
	// 而 AsyncDo 要碰读闸与 poller 注册。需要的话先 Post 排回去。
	err := c.AsyncDo(func() {
		// 这里可以安全地访问 c.State：gate 保证它与其余回调不重叠。
		c.State.room = lookupRoomFromDB(c.State.id)
	})
	if errors.Is(err, gate.ErrAsyncBusy) {
		fmt.Println("该连接已有一次 AsyncDo 在途")
	}
}

func lookupRoomFromDB(uint64) string { return "" }
