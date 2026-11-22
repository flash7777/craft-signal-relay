// Package signal ist das Relais von Craft Signal (SIGNAL.md): Ticketausgabe, Sitzungsregister mit Rangfolge,
// Steuer- und Bulk-Kanal über WebSocket. Es liest vom Rahmen nur den Kopf und die eigenen Nutzlasten
// (HELLO, MODE, BLOB_*); OP, COMMIT, NAK, SYNC und PRESENCE gehen ungelesen weiter.
package signal

import (
	"encoding/binary"
	"errors"

	"github.com/fxamacker/cbor/v2"
)

// Modi eines Teilnehmers.
const (
	ModeReadOnly uint8 = 0
	ModeEdit     uint8 = 1
)

// NoRank hat, wer nur liest.
const NoRank uint16 = 0xFFFF

// HelloPayload ist die Nutzlast von HELLO (Client → Relais), CBOR.
type HelloPayload struct {
	Ticket     string `cbor:"t"`
	App        string `cbor:"a"`
	AppVersion string `cbor:"v"`
	Mode       uint8  `cbor:"m"`
	// Bulk meldet die zweite Verbindung desselben Teilnehmers an (Bulk-Kanal).
	Bulk bool `cbor:"b,omitempty"`
}

// WelcomePayload ist die Nutzlast von WELCOME (Relais → Client); Sitzung, Teilnehmernummer und `seq` stehen im Kopf.
type WelcomePayload struct {
	Rank       uint16 `cbor:"r"`
	Master     uint16 `cbor:"M"`
	Mode       uint8  `cbor:"m"`
	App        string `cbor:"a"`
	AppVersion string `cbor:"v"`
	// Reason sagt, warum der Modus kleiner ist als gewünscht (`appVersion`, `rights`).
	Reason string `cbor:"x,omitempty"`
	// Lock ist die Sperrkennung für den PUT; nur der Master bekommt sie.
	Lock string `cbor:"l,omitempty"`
}

// RosterEntry ist ein Teilnehmer, wie ROSTER ihn nennt.
type RosterEntry struct {
	ID   uint16 `cbor:"i"`
	Rank uint16 `cbor:"r"`
	Mode uint8  `cbor:"m"`
	Name string `cbor:"n,omitempty"`
}

// RosterPayload ist die Nutzlast von ROSTER (Relais → alle).
type RosterPayload struct {
	Members []RosterEntry `cbor:"p"`
	Master  uint16        `cbor:"M"`
}

// ModePayload ist die Nutzlast von MODE (Client → Relais) und der Antwort (Relais → Client, gewährter Modus).
type ModePayload struct {
	Mode   uint8  `cbor:"m"`
	Reason string `cbor:"x,omitempty"`
}

// MasterPayload ist die Nutzlast von MASTER (Relais → alle); `seq` im Kopf ist der letzte bekannte Stand.
type MasterPayload struct {
	Master uint16 `cbor:"M"`
	Lock   string `cbor:"l,omitempty"`
}

// Encode schreibt eine Nutzlast als CBOR.
func Encode(v any) ([]byte, error) {
	return cbor.Marshal(v)
}

// DecodePayload liest eine CBOR-Nutzlast.
func DecodePayload(data []byte, v any) error {
	return cbor.Unmarshal(data, v)
}

// HashSize ist die Länge des Blob-Hashes (BLAKE3 oder SHA-256, 32 Byte).
const HashSize = 32

// BlobHeaderSize ist der Vorspann eines BLOB_PART: Hash, Offset u64, Länge u32.
const BlobHeaderSize = HashSize + 8 + 4

// ErrBlobHeader meldet einen Bulk-Rahmen ohne gültigen Vorspann.
var ErrBlobHeader = errors.New("signal: blob frame without a valid header")

// BlobPart ist ein Teil eines Blobs, wie BLOB_PART ihn trägt.
type BlobPart struct {
	Hash   [HashSize]byte
	Offset uint64
	Data   []byte
}

// DecodeBlobPart liest den Vorspann und die Bytes eines BLOB_PART.
func DecodeBlobPart(payload []byte) (*BlobPart, error) {
	if len(payload) < BlobHeaderSize {
		return nil, ErrBlobHeader
	}
	length := binary.LittleEndian.Uint32(payload[HashSize+8:])
	if int(length) != len(payload)-BlobHeaderSize {
		return nil, ErrBlobHeader
	}
	part := &BlobPart{Offset: binary.LittleEndian.Uint64(payload[HashSize:]), Data: append([]byte(nil), payload[BlobHeaderSize:]...)}
	copy(part.Hash[:], payload[:HashSize])
	return part, nil
}

// EncodeBlobPart schreibt einen BLOB_PART.
func EncodeBlobPart(part *BlobPart) []byte {
	out := make([]byte, BlobHeaderSize+len(part.Data))
	copy(out, part.Hash[:])
	binary.LittleEndian.PutUint64(out[HashSize:], part.Offset)
	binary.LittleEndian.PutUint32(out[HashSize+8:], uint32(len(part.Data)))
	copy(out[BlobHeaderSize:], part.Data)
	return out
}

// BlobRequest ist die Nutzlast von BLOB_REQ: der Hash; ohne Bereiche heißt es „alles“.
type BlobRequest struct {
	Hash [HashSize]byte
}

// DecodeBlobRequest liest ein BLOB_REQ (Hash, optional gefolgt von Bereichen, die das Relais nicht auswertet).
func DecodeBlobRequest(payload []byte) (*BlobRequest, error) {
	if len(payload) < HashSize {
		return nil, ErrBlobHeader
	}
	req := &BlobRequest{}
	copy(req.Hash[:], payload[:HashSize])
	return req, nil
}
