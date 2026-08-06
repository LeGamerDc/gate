package gate

import "testing"

func TestTokenPackUnpack(t *testing.T) {
	cases := []struct {
		kind tokenKind
		gen  uint32
		slot uint32
	}{
		{tokConn, 0, 0},
		{tokConn, 1, 42},
		{tokListener, genMask, 0xFFFFFFFF}, // gen 30 位全 1，slot 32 位全 1
		{tokNotify, 12345, 67890},
		{tokConn, genMask + 1, 7}, // gen 溢出 30 位：截断，不污染 kind
	}
	for _, tc := range cases {
		tok := makeToken(tc.kind, tc.gen, tc.slot)
		if tok.kind() != tc.kind {
			t.Fatalf("kind: got %d want %d", tok.kind(), tc.kind)
		}
		if tok.gen() != tc.gen&genMask {
			t.Fatalf("gen: got %d want %d", tok.gen(), tc.gen&genMask)
		}
		if tok.slot() != tc.slot {
			t.Fatalf("slot: got %d want %d", tok.slot(), tc.slot)
		}
	}
	if notifyToken.kind() != tokNotify {
		t.Fatal("notifyToken kind")
	}
}

// gen 递增让陈旧 token 失效（R5 的机制基础）：同槽位不同 gen 的 token 不相等。
func TestTokenGenerationDistinguishes(t *testing.T) {
	a := makeToken(tokConn, 1, 5)
	b := makeToken(tokConn, 2, 5)
	if a == b || a.slot() != b.slot() {
		t.Fatal("同槽位不同 gen 应产生不同 token")
	}
}
