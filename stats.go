package gate

import (
	"errors"
	"sync/atomic"
)

// Stats 是运行统计快照。计数器每事件循环独立（无跨核往返），读取时求和。
// 最值得观察的两个比值：BytesOut/BytesOutRaw（压缩率）、
// MessagesOut/FramesOut（合包聚合度）。
type Stats struct {
	// 连接
	ConnsOpen          int64  // 当前
	ConnsHandshaking   int64  // 当前处于握手阶段（WebSocket / PROXY）
	ConnsPaused        int64  // 当前被 Pause
	ConnsOverHighWater int64  // 当前 Writable() == false（未逐条采样，读取时为 0）
	ConnsAccepted      uint64 // 累计
	ConnsRejected      uint64

	// 流量
	MessagesIn  uint64
	MessagesOut uint64
	BytesIn     uint64
	BytesOut    uint64 // 线路字节（压缩后）
	BytesOutRaw uint64 // 压缩前
	FramesOut   uint64 // 线路帧数

	// 资源水位
	OutboundQueued int64 // 当前出站积压总字节（reservedWire 口径）
	PendingInbound int64 // 当前入站残片总字节
	// PoolMiss 是**进程级**的：分级池被所有 server 共享，同进程起多个
	// server 时这个数不区分来源。稳态下它应该基本不涨。
	PoolMiss uint64

	// 压力信号
	WriteEAGAIN   uint64 // writev 写不完的次数
	SendQueueFull uint64 // Send 因准入上限被拒的次数
	LoopLagNanos  uint64 // 一轮迭代耗时累计，除以迭代数即平均循环延迟

	// 关闭原因分布（完整覆盖 OnClose 的 reason 表）
	ClosedPeer, ClosedIdle, ClosedBackpressure, ClosedProtocol,
	ClosedPanic, ClosedPauseTimeout, ClosedHandshakeTimeout,
	ClosedPendingOverflow, ClosedByServer, ClosedByHandler uint64
}

// loopStats 是每 loop 一份的计数器。Send 可能从任意 goroutine 记数，
// 全部用原子操作——无争用时成本可忽略。
type loopStats struct {
	connsOpen        atomic.Int64
	connsHandshaking atomic.Int64
	connsPaused      atomic.Int64
	connsAccepted    atomic.Uint64
	connsRejected    atomic.Uint64

	messagesIn  atomic.Uint64
	messagesOut atomic.Uint64
	bytesIn     atomic.Uint64
	bytesOut    atomic.Uint64
	bytesOutRaw atomic.Uint64
	framesOut   atomic.Uint64

	outboundQueued     atomic.Int64
	pendingInbound     atomic.Int64
	connsOverHighWater atomic.Int64

	writeEAGAIN   atomic.Uint64
	sendQueueFull atomic.Uint64
	loopLagNanos  atomic.Uint64

	closedPeer, closedIdle, closedBackpressure, closedProtocol,
	closedPanic, closedPauseTimeout, closedHandshakeTimeout,
	closedPendingOverflow, closedByServer, closedByHandler atomic.Uint64
}

func (s *loopStats) snapshot() Stats {
	return Stats{
		ConnsOpen:        s.connsOpen.Load(),
		ConnsHandshaking: s.connsHandshaking.Load(),
		ConnsPaused:      s.connsPaused.Load(),
		ConnsAccepted:    s.connsAccepted.Load(),
		ConnsRejected:    s.connsRejected.Load(),

		MessagesIn:  s.messagesIn.Load(),
		MessagesOut: s.messagesOut.Load(),
		BytesIn:     s.bytesIn.Load(),
		BytesOut:    s.bytesOut.Load(),
		BytesOutRaw: s.bytesOutRaw.Load(),
		FramesOut:   s.framesOut.Load(),

		ConnsOverHighWater: s.connsOverHighWater.Load(),
		OutboundQueued:     s.outboundQueued.Load(),
		PendingInbound:     s.pendingInbound.Load(),

		WriteEAGAIN:   s.writeEAGAIN.Load(),
		SendQueueFull: s.sendQueueFull.Load(),
		LoopLagNanos:  s.loopLagNanos.Load(),

		ClosedPeer:             s.closedPeer.Load(),
		ClosedIdle:             s.closedIdle.Load(),
		ClosedBackpressure:     s.closedBackpressure.Load(),
		ClosedProtocol:         s.closedProtocol.Load(),
		ClosedPanic:            s.closedPanic.Load(),
		ClosedPauseTimeout:     s.closedPauseTimeout.Load(),
		ClosedHandshakeTimeout: s.closedHandshakeTimeout.Load(),
		ClosedPendingOverflow:  s.closedPendingOverflow.Load(),
		ClosedByServer:         s.closedByServer.Load(),
		ClosedByHandler:        s.closedByHandler.Load(),
	}
}

func addStats(dst *Stats, s Stats) {
	dst.ConnsOpen += s.ConnsOpen
	dst.ConnsHandshaking += s.ConnsHandshaking
	dst.ConnsPaused += s.ConnsPaused
	dst.ConnsAccepted += s.ConnsAccepted
	dst.ConnsRejected += s.ConnsRejected
	dst.MessagesIn += s.MessagesIn
	dst.MessagesOut += s.MessagesOut
	dst.BytesIn += s.BytesIn
	dst.BytesOut += s.BytesOut
	dst.BytesOutRaw += s.BytesOutRaw
	dst.FramesOut += s.FramesOut
	dst.OutboundQueued += s.OutboundQueued
	dst.PendingInbound += s.PendingInbound
	dst.ConnsOverHighWater += s.ConnsOverHighWater
	dst.WriteEAGAIN += s.WriteEAGAIN
	dst.SendQueueFull += s.SendQueueFull
	dst.LoopLagNanos += s.LoopLagNanos
	dst.ClosedPeer += s.ClosedPeer
	dst.ClosedIdle += s.ClosedIdle
	dst.ClosedBackpressure += s.ClosedBackpressure
	dst.ClosedProtocol += s.ClosedProtocol
	dst.ClosedPanic += s.ClosedPanic
	dst.ClosedPauseTimeout += s.ClosedPauseTimeout
	dst.ClosedHandshakeTimeout += s.ClosedHandshakeTimeout
	dst.ClosedPendingOverflow += s.ClosedPendingOverflow
	dst.ClosedByServer += s.ClosedByServer
	dst.ClosedByHandler += s.ClosedByHandler
}

// countClose 把关闭原因归入分布（顺序即优先级：先具体后笼统）。
func (s *loopStats) countClose(reason error) {
	switch {
	case reason == nil:
		s.closedByHandler.Add(1)
	case errors.Is(reason, ErrPeerClosed), errors.Is(reason, ErrPeerReset):
		s.closedPeer.Add(1)
	case errors.Is(reason, ErrIdleTimeout):
		s.closedIdle.Add(1)
	case errors.Is(reason, ErrBackpressure):
		s.closedBackpressure.Add(1)
	case errors.Is(reason, ErrProtocol):
		s.closedProtocol.Add(1)
	case errors.Is(reason, ErrHandlerPanic):
		s.closedPanic.Add(1)
	case errors.Is(reason, ErrPauseTimeout):
		s.closedPauseTimeout.Add(1)
	case errors.Is(reason, ErrHandshakeTimeout):
		s.closedHandshakeTimeout.Add(1)
	case errors.Is(reason, ErrPendingOverflow):
		s.closedPendingOverflow.Add(1)
	case errors.Is(reason, ErrServerClosed):
		s.closedByServer.Add(1)
	default:
		s.closedByHandler.Add(1)
	}
}
