package gate

import (
	"errors"
)

// WS 与 loop 的接线（05「分层位置」）：WS 帧层只做字节变换，
// 下游是与裸 TCP 完全相同的 gate 帧循环（W1）。

// errWSStop：feed 的下游要求停止（连接已关闭/进入关闭流程）。
// 不是错误——关闭已在别处发起，feed 只需要放下剩余字节。
var errWSStop = errors.New("gate/ws: stop feeding")

// enableWS 把连接切到 WebSocket 模式（握手接受后调用；测试直接调）。
func enableWS(c *connCore) {
	c.ws = &wsState{}
	c.cb.onDrain = wsOnDrain(c)
	l := c.loop
	c.wsEmitFn = func(seg []byte) error { return l.wsGateFeed(c, seg) }
	c.wsCtrlFn = func(op byte, payload []byte) error { return l.wsCtrl(c, op, payload) }
}

// wsOnDrain 是进入 Draining 时的钩子：按 reason 决定发不发 close 帧（W10）。
// 在 closeSend 之前被调用，close 帧因此还能排进出站链尾。
func wsOnDrain(c *connCore) func(error) {
	return func(reason error) {
		w := c.ws
		if w == nil || w.closeSent {
			return
		}
		code, ok := wsCloseCodeFor(reason)
		if !ok {
			return // 背压/写失败/对端已断：直接关 TCP
		}
		// force：此刻 closing 已由 beginClose 置位，业务数据再也进不来，
		// 所以这一帧结构上必然是 FIFO 的最后一段（W13）。
		if c.out.sendRaw(wsCloseFrame(code, ""), true) == nil {
			w.closeSent = true
		}
	}
}

// wsReadable 处理 WS 连接（Open 状态）的读事件。
// 调用方（connReadable）已设好恢复屏障与批边界 flush。
func (l *loop) wsReadable(c *connCore) {
	calls, bytes := 0, 0
	for calls < readBudgetCalls && bytes < readBudgetBytes {
		if c.state != stateOpen || c.pauseDepth > 0 || c.closing.Load() {
			return
		}
		n, err := l.io.read(c.fd, l.rbuf)
		if !l.readOK(c, n, err) {
			return
		}
		calls++
		bytes += n
		l.stats.bytesIn.Add(uint64(n))
		if !l.wsFeed(c, l.rbuf[:n]) {
			return
		}
	}
	l.truncated = true
}

// wsFeed 把一段原始字节推进 WS 帧层。返回 false 表示本轮到此为止。
func (l *loop) wsFeed(c *connCore, raw []byte) bool {
	err := c.ws.feed(raw, c.wsEmitFn, c.wsCtrlFn)
	if err != nil {
		if !errors.Is(err, errWSStop) {
			l.closeLocal(c, wrapProtocol(err))
		}
		return false
	}
	return true
}

// wsGateFeed 是 feed 的 emit 回调：把一段 gate 字节流交给 gate 帧循环。
// 与 TCP 路径共用 parseAndDeliver / stashCarry / 大帧直读——「语义一致」
// 是同一份代码（W1）。暂停不停止 feed：内核已读出的字节转 carry 存住
// （MaxPending 封顶），resume 后经 processCarry 继续投递。
func (l *loop) wsGateFeed(c *connCore, seg []byte) error {
	// 大帧直读中：seg 直接填 frameBuf 的剩余区。
	for c.in.body != nil {
		if c.state != stateOpen || c.closing.Load() {
			return errWSStop
		}
		if len(seg) == 0 {
			return nil
		}
		take := min(len(c.in.body)-c.in.got, len(seg))
		copy(c.in.body[c.in.got:], seg[:take])
		c.in.got += take
		seg = seg[take:]
		if c.in.got == len(c.in.body) {
			body := c.in.body
			c.in.body, c.in.got = nil, 0
			l.deliverFrameBuf(c, body)
		}
	}
	if len(seg) == 0 {
		return nil
	}
	if c.state != stateOpen || c.closing.Load() {
		return errWSStop
	}

	data := seg
	var merged []byte
	if c.in.carry != nil {
		// gate 残片拼接（对应 TCP 路径的「carry 拷回 rbuf 头部」）。
		need := len(c.in.carry) + len(seg)
		if cap(c.in.carry) >= need {
			merged = append(c.in.carry, seg...)
		} else {
			merged = append(poolGet(need)[:0], c.in.carry...)
			merged = append(merged, seg...)
			poolPut(c.in.carry)
		}
		c.in.carry = nil
		c.in.syncPending(l.stats)
		data = merged
	}
	// 暂停 / 投递预算截断 / 大帧接管都在 parseAndDeliver 内部 stash 进 carry。
	// 这三种情况下**仍然继续喂入后续原始字节**：feed 已经消费掉的部分不可能
	// 回退，中途放手会让 maskOff 相位与 WS 帧边界一起错位。安全性由两侧封顶
	// 保证——carry 受 MaxPending 约束（超限即 ErrPendingOverflow 关闭），
	// 而 processCarry 直接在 carry 上解析，不再假设它装得进 rbuf。
	l.parseAndDeliver(c, data)
	if merged != nil {
		poolPut(merged)
	}
	if c.state != stateOpen || c.closing.Load() {
		return errWSStop
	}
	return nil
}

// wsCtrl 处理收齐的控制帧。
func (l *loop) wsCtrl(c *connCore, op byte, payload []byte) error {
	w := c.ws
	switch op {
	case wsOpPing:
		// pong 走正常准入：ping 洪水下 pong 被 ErrSendQueueFull 挡掉即可，
		// 不值得为它突破 MaxBuffer（对端本来就没在读）。
		_ = c.out.sendRaw(wsControlFrame(wsOpPong, payload), false)
	case wsOpPong:
		// 我们不主动 ping；对端的 pong 忽略。
	case wsOpClose:
		code, _, err := parseClosePayload(payload)
		if err != nil {
			return err // 协议违规：非法 code / 非 UTF-8 reason 留在错误路径上
		}
		w.closeRecv = true
		if !w.closeSent {
			// 回 close 完成握手。1005 表示对端没带 code，回帧也不带。
			var frame []byte
			if code == 1005 {
				frame = wsControlFrame(wsOpClose, nil)
			} else {
				frame = wsCloseFrame(code, "")
			}
			if c.out.sendRaw(frame, false) == nil {
				w.closeSent = true
			}
		}
		// 正常关闭翻译成 ErrPeerClosed——不是错误，不该刷 warn 日志（05）。
		l.closeLocal(c, ErrPeerClosed)
		return errWSStop
	}
	return nil
}
