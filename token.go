package gate

// token 是 poller 事件的内部标识，随事件原样返回，事件分发零查表、零哈希。
//
//	位段：kind(2) | generation(30) | slot(32)
//
// 槽位会复用，token 因 generation 而不会：事件带回的 token 与槽位当前 gen
// 对不上就是陈旧事件，直接丢弃（R5）。注意它与公开的 Conn.ID() 无关——
// 后者必须永不复用（R6），混用会出现注册表 key 撞车（R-D4）。
type token uint64

type tokenKind uint8

const (
	tokConn     tokenKind = 0
	tokListener tokenKind = 1
	tokNotify   tokenKind = 2
)

const genMask = 1<<30 - 1

func makeToken(kind tokenKind, gen uint32, slot uint32) token {
	return token(kind)<<62 | token(gen&genMask)<<32 | token(slot)
}

func (t token) kind() tokenKind { return tokenKind(t >> 62) }
func (t token) gen() uint32     { return uint32(t>>32) & genMask }
func (t token) slot() uint32    { return uint32(t) }

// notifyToken 是唤醒事件的固定标识。
const notifyToken = token(tokNotify) << 62
