// Package frame ist das Rahmenformat von Craft Signal (SIGNAL.md): ein fester Kopf von 24 Byte, Little Endian,
// und eine Nutzlast, die das Relais nicht liest. Der Kopf trägt alles, was das Relais zum Verteilen braucht.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Version ist die Protokollversion dieses Pakets.
const Version uint8 = 1

// HeaderSize ist die Länge des Kopfes in Byte.
const HeaderSize = 24

// MaxControlPayload ist die Grenze der Nutzlast auf dem Steuerkanal (ein Rahmen je TCP-Segment, SIGNAL.md Transport).
const MaxControlPayload = 1300

// MaxBulkPayload ist die Grenze eines Teilrahmens auf dem Bulk-Kanal.
const MaxBulkPayload = 64 * 1024

// Type ist der Nachrichtentyp eines Rahmens.
type Type uint8

// Die Typen aus der Tabelle in SIGNAL.md; 20 aufwärts nur auf dem Bulk-Kanal.
const (
	Hello    Type = 1
	Welcome  Type = 2
	Roster   Type = 3
	SyncReq  Type = 4
	Sync     Type = 5
	Op       Type = 6
	Commit   Type = 7
	Nak      Type = 8
	Presence Type = 9
	Mode     Type = 10
	Master   Type = 11
	Saved    Type = 12
	Ping     Type = 13
	Pong     Type = 14
	Bye      Type = 15
	// SaveReq bittet den Master zu speichern (ein Teilnehmer, der kein Master ist), SaveFail ist seine Absage.
	SaveReq  Type = 16
	SaveFail Type = 17
	BlobPart Type = 20
	BlobReq  Type = 21
	BlobDone Type = 22
)

var typeNames = map[Type]string{
	Hello: "HELLO", Welcome: "WELCOME", Roster: "ROSTER", SyncReq: "SYNC_REQ", Sync: "SYNC", Op: "OP", Commit: "COMMIT",
	Nak: "NAK", Presence: "PRESENCE", Mode: "MODE", Master: "MASTER", Saved: "SAVED", Ping: "PING", Pong: "PONG", Bye: "BYE",
	SaveReq: "SAVE_REQ", SaveFail: "SAVE_FAIL",
	BlobPart: "BLOB_PART", BlobReq: "BLOB_REQ", BlobDone: "BLOB_DONE",
}

func (t Type) String() string {
	if name, ok := typeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("Type(%d)", uint8(t))
}

// Bulk sagt, ob der Typ auf den Bulk-Kanal gehört.
func (t Type) Bulk() bool {
	return t >= BlobPart
}

// Frame ist ein Rahmen: Kopf und Nutzlast.
type Frame struct {
	Ver     uint8
	Type    Type
	Flags   uint16
	Session uint32
	Sender  uint16
	Seq     uint32
	Base    uint32
	Local   uint32
	Payload []byte
}

// ErrShort meldet einen Rahmen, der kürzer als sein Kopf ist.
var ErrShort = errors.New("frame: shorter than its header")

// ErrLength meldet einen Kopf, dessen Länge nicht zur Nutzlast passt.
var ErrLength = errors.New("frame: length in the header does not match the payload")

// ErrVersion meldet eine fremde Protokollversion.
var ErrVersion = errors.New("frame: unknown protocol version")

// Encode schreibt Kopf und Nutzlast als einen binären Rahmen. Eine Nutzlast über 65535 Byte passt nicht in `len`.
func (f *Frame) Encode() ([]byte, error) {
	if len(f.Payload) > 0xFFFF {
		return nil, fmt.Errorf("frame: payload of %d bytes does not fit the header", len(f.Payload))
	}
	out := make([]byte, HeaderSize+len(f.Payload))
	ver := f.Ver
	if ver == 0 {
		ver = Version
	}
	out[0] = ver
	out[1] = uint8(f.Type)
	binary.LittleEndian.PutUint16(out[2:], f.Flags)
	binary.LittleEndian.PutUint32(out[4:], f.Session)
	binary.LittleEndian.PutUint16(out[8:], f.Sender)
	binary.LittleEndian.PutUint32(out[10:], f.Seq)
	binary.LittleEndian.PutUint32(out[14:], f.Base)
	binary.LittleEndian.PutUint32(out[18:], f.Local)
	binary.LittleEndian.PutUint16(out[22:], uint16(len(f.Payload)))
	copy(out[HeaderSize:], f.Payload)
	return out, nil
}

// Decode liest einen Rahmen. Die Nutzlast ist eine Kopie, der Rahmen hängt nicht am Puffer.
func Decode(data []byte) (*Frame, error) {
	if len(data) < HeaderSize {
		return nil, ErrShort
	}
	if data[0] != Version {
		return nil, ErrVersion
	}
	length := int(binary.LittleEndian.Uint16(data[22:]))
	if len(data)-HeaderSize != length {
		return nil, ErrLength
	}
	f := &Frame{
		Ver:     data[0],
		Type:    Type(data[1]),
		Flags:   binary.LittleEndian.Uint16(data[2:]),
		Session: binary.LittleEndian.Uint32(data[4:]),
		Sender:  binary.LittleEndian.Uint16(data[8:]),
		Seq:     binary.LittleEndian.Uint32(data[10:]),
		Base:    binary.LittleEndian.Uint32(data[14:]),
		Local:   binary.LittleEndian.Uint32(data[18:]),
		Payload: append([]byte(nil), data[HeaderSize:]...),
	}
	return f, nil
}
