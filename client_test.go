package gate

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestClientSendDoesNotCompressOrCompound(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverConnCh := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			serverConnCh <- conn
		}
	}()

	handler := &testClientHandler{}
	cipher := xorCipher{key: 0x5a}
	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:    ln.Addr().String(),
		Handler: handler,
		Cipher:  cipher,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = client.Wait()
	}()

	serverConn := <-serverConnCh
	defer serverConn.Close()

	msg1 := bytes.Repeat([]byte("a"), 5000)
	msg2 := []byte("plain")

	if err := client.Send(msg1); err != nil {
		t.Fatal(err)
	}
	if err := client.SendNoEncrypt(msg2); err != nil {
		t.Fatal(err)
	}

	frame1 := readTestFrame(t, serverConn)
	if frame1.z || frame1.c {
		t.Fatalf("unexpected outbound flags: z=%v c=%v", frame1.z, frame1.c)
	}
	if !frame1.e {
		t.Fatal("expected encrypted outbound frame")
	}
	cipher.Decrypt(frame1.payload)
	if !bytes.Equal(frame1.payload, msg1) {
		t.Fatal("encrypted outbound payload mismatch")
	}

	frame2 := readTestFrame(t, serverConn)
	if frame2.z || frame2.c || frame2.e {
		t.Fatalf("unexpected plaintext outbound flags: z=%v c=%v e=%v", frame2.z, frame2.c, frame2.e)
	}
	if !bytes.Equal(frame2.payload, msg2) {
		t.Fatal("plaintext outbound payload mismatch")
	}
}

func TestClientReceiveCompressedCompoundEncrypted(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverConnCh := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			serverConnCh <- conn
		}
	}()

	handler := &testClientHandler{
		msgCh:     make(chan []byte, 2),
		closeCh:   make(chan error, 1),
		connectCh: make(chan struct{}, 1),
	}
	cipher := xorCipher{key: 0x33}
	client, err := DialContext(context.Background(), &ClientConfig{
		Addr:    ln.Addr().String(),
		Handler: handler,
		Cipher:  cipher,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = client.Wait()
	}()

	serverConn := <-serverConnCh
	defer serverConn.Close()

	msg1 := []byte("hello")
	msg2 := bytes.Repeat([]byte("b"), 6000)
	payload := buildCompoundPayload(msg1, msg2)
	compressed := enc.EncodeAll(payload, nil)
	cipher.Encrypt(compressed)
	if _, err := serverConn.Write(buildFrame(compressed, maskZ|maskC|maskE)); err != nil {
		t.Fatal(err)
	}

	got1 := waitMessage(t, handler.msgCh)
	got2 := waitMessage(t, handler.msgCh)
	if !bytes.Equal(got1, msg1) {
		t.Fatalf("unexpected first message: %q", got1)
	}
	if !bytes.Equal(got2, msg2) {
		t.Fatal("unexpected second message")
	}
}

type testClientHandler struct {
	msgCh     chan []byte
	closeCh   chan error
	connectCh chan struct{}
	once      sync.Once
}

func (h *testClientHandler) OnConnect(*Client) {
	if h.connectCh != nil {
		h.connectCh <- struct{}{}
	}
}

func (h *testClientHandler) OnMessage(_ *Client, msg []byte) {
	if h.msgCh != nil {
		h.msgCh <- msg
	}
}

func (h *testClientHandler) OnClose(_ *Client, err error) {
	if h.closeCh != nil {
		h.once.Do(func() {
			h.closeCh <- err
		})
	}
}

type xorCipher struct {
	key byte
}

func (c xorCipher) Encrypt(data []byte) {
	for i := range data {
		data[i] ^= c.key
	}
}

func (c xorCipher) Decrypt(data []byte) {
	c.Encrypt(data)
}

func buildCompoundPayload(msgs ...[]byte) []byte {
	var buf bytes.Buffer
	for _, msg := range msgs {
		buf.Write(buildFrame(msg, 0))
	}
	return buf.Bytes()
}

func buildFrame(payload []byte, flag byte) []byte {
	var header [4]byte
	n := encodeHeader(header[:], len(payload))
	header[0] |= flag
	frame := make([]byte, n+len(payload))
	copy(frame, header[:n])
	copy(frame[n:], payload)
	return frame
}

func readTestFrame(t *testing.T, conn net.Conn) inboundFrame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := readFrame(conn, maxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func waitMessage(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return nil
	}
}

func TestConsumeFrameRejectsTruncatedPayload(t *testing.T) {
	frame := buildFrame([]byte("abc"), 0)
	_, _, err := consumeFrame(frame[:len(frame)-1], maxMessageSize)
	if err == nil {
		t.Fatal("expected truncated frame error")
	}
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("unexpected error: %v", err)
	}
}
