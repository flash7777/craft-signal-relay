package frame

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := &Frame{Type: Commit, Flags: 3, Session: 7, Sender: 2, Seq: 100, Base: 99, Local: 5, Payload: []byte("payload")}
	data, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != HeaderSize+7 {
		t.Fatalf("length %d", len(data))
	}
	out, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if out.Ver != Version || out.Type != Commit || out.Flags != 3 || out.Session != 7 || out.Sender != 2 || out.Seq != 100 || out.Base != 99 || out.Local != 5 {
		t.Fatalf("header %+v", out)
	}
	if !bytes.Equal(out.Payload, []byte("payload")) {
		t.Fatalf("payload %q", out.Payload)
	}
	// Ein Rahmen ohne Nutzlast ist genau der Kopf.
	empty, _ := (&Frame{Type: Ping}).Encode()
	if len(empty) != HeaderSize {
		t.Fatalf("empty %d", len(empty))
	}
}

func TestDecodeRefusesBadFrames(t *testing.T) {
	if _, err := Decode(make([]byte, HeaderSize-1)); err != ErrShort {
		t.Fatalf("short: %v", err)
	}
	data, _ := (&Frame{Type: Op, Payload: []byte("abc")}).Encode()
	if _, err := Decode(data[:len(data)-1]); err != ErrLength {
		t.Fatalf("length: %v", err)
	}
	data[0] = 9
	if _, err := Decode(data); err != ErrVersion {
		t.Fatalf("version: %v", err)
	}
	if _, err := (&Frame{Type: Op, Payload: make([]byte, 70000)}).Encode(); err == nil {
		t.Fatal("a payload over 65535 bytes must not encode")
	}
}

func TestTypeNamesAndChannels(t *testing.T) {
	if Op.String() != "OP" || BlobPart.String() != "BLOB_PART" || Type(99).String() != "Type(99)" {
		t.Fatal("names")
	}
	if Op.Bulk() || !BlobReq.Bulk() {
		t.Fatal("channels")
	}
}
