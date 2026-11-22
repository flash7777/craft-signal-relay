package signal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/flash7777/craft-signal-relay/frame"
)

// client ist ein Testteilnehmer: Ticket, Steuerkanal, auf Wunsch Bulk-Kanal.
type client struct {
	t       *testing.T
	server  *httptest.Server
	user    string
	ticket  TicketResponse
	control *websocket.Conn
	bulk    *websocket.Conn
	// Alles, was auf dem Steuer- und dem Bulk-Kanal ankommt, gelesen von je einer Goroutine: ein Lesen mit Frist
	// würde die Verbindung schließen (coder/websocket), ein Kanal nicht.
	frames  chan *frame.Frame
	bulkIn  chan *frame.Frame
	id      uint16
	session uint32
	welcome WelcomePayload
}

// pump liest einen WebSocket in einen Kanal, bis die Verbindung endet.
func pump(ws *websocket.Conn, out chan *frame.Frame) {
	for {
		_, data, err := ws.Read(context.Background())
		if err != nil {
			close(out)
			return
		}
		if f, err := frame.Decode(data); err == nil {
			out <- f
		}
	}
}

func newServer(t *testing.T) (*Hub, *httptest.Server) {
	t.Helper()
	hub := New(Options{Dev: true, BlobBudget: 1 << 20})
	mux := http.NewServeMux()
	hub.Routes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return hub, server
}

func (c *client) getTicket(doc, app, version string, mode uint8) {
	c.t.Helper()
	body, _ := json.Marshal(TicketRequest{Doc: doc, App: app, AppVersion: version, Mode: mode, Name: c.user})
	req, _ := http.NewRequest("POST", c.server.URL+"/signal/ticket", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-"+c.user)
	req.Header.Set("X-Signal-User", c.user)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		c.t.Fatalf("ticket: %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&c.ticket); err != nil {
		c.t.Fatal(err)
	}
}

func dial(t *testing.T, server *httptest.Server, path string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, strings.Replace(server.URL, "http://", "ws://", 1)+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func send(t *testing.T, ws *websocket.Conn, f *frame.Frame) {
	t.Helper()
	data, err := f.Encode()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ws.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatal(err)
	}
}

// recvType wartet auf einen Rahmen des Typs; andere (ROSTER und Nebenrahmen) werden übersprungen.
func recvType(t *testing.T, in chan *frame.Frame, want frame.Type) *frame.Frame {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case f, ok := <-in:
			if !ok {
				t.Fatalf("connection closed while waiting for %s", want)
			}
			if f.Type == want {
				return f
			}
		case <-deadline:
			t.Fatalf("no %s frame", want)
		}
	}
}

// nothing stellt sicher, dass innerhalb der Frist kein Rahmen des Typs kommt.
func nothing(t *testing.T, in chan *frame.Frame, forbidden frame.Type, wait time.Duration) {
	t.Helper()
	deadline := time.After(wait)
	for {
		select {
		case f, ok := <-in:
			if ok && f.Type == forbidden {
				t.Fatalf("unexpected %s frame", forbidden)
			}
			if !ok {
				return
			}
		case <-deadline:
			return
		}
	}
}

// join holt ein Ticket, verbindet den Steuerkanal und wertet WELCOME aus.
func join(t *testing.T, server *httptest.Server, user, doc, app, version string, mode uint8) *client {
	t.Helper()
	c := &client{t: t, server: server, user: user, frames: make(chan *frame.Frame, 256), bulkIn: make(chan *frame.Frame, 256)}
	// Das Ticket erlaubt das Bearbeiten (Rechte), der HELLO nennt den Modus, mit dem der Client beginnt.
	c.getTicket(doc, app, version, ModeEdit)
	c.control = dial(t, server, "/signal")
	go pump(c.control, c.frames)
	payload, _ := Encode(HelloPayload{Ticket: c.ticket.Ticket, App: app, AppVersion: version, Mode: mode})
	send(t, c.control, &frame.Frame{Type: frame.Hello, Payload: payload})
	w := recvType(t, c.frames, frame.Welcome)
	if err := DecodePayload(w.Payload, &c.welcome); err != nil {
		t.Fatal(err)
	}
	c.id, c.session = w.Sender, w.Session
	return c
}

func (c *client) openBulk() {
	c.t.Helper()
	c.bulk = dial(c.t, c.server, "/signal/bulk")
	go pump(c.bulk, c.bulkIn)
	payload, _ := Encode(HelloPayload{Ticket: c.ticket.Ticket, Bulk: true})
	send(c.t, c.bulk, &frame.Frame{Type: frame.Hello, Payload: payload})
	recvType(c.t, c.bulkIn, frame.Welcome)
}

func TestFirstEditorIsMasterAndReadersMaySwitch(t *testing.T) {
	hub, server := newServer(t)
	a := join(t, server, "anna", "doc-1", "wordcraft", "0.18.1-kosmos.5", ModeEdit)
	if a.welcome.Mode != ModeEdit || a.welcome.Rank != 0 || a.welcome.Master != a.id || a.welcome.Lock == "" {
		t.Fatalf("first editor is the master with the lock: %+v", a.welcome)
	}
	b := join(t, server, "ben", "doc-1", "wordcraft", "0.18.1-kosmos.5", ModeReadOnly)
	if b.welcome.Mode != ModeReadOnly || b.welcome.Rank != NoRank || b.welcome.Master != a.id || b.welcome.Lock != "" {
		t.Fatalf("reader: %+v", b.welcome)
	}
	if b.ticket.Session != a.session || b.ticket.App != "wordcraft" {
		t.Fatalf("the ticket names the running session: %+v", b.ticket)
	}
	if hub.Sessions() != 1 {
		t.Fatal("one session")
	}
	// Umschalten auf Kollaboration: Rang 1, ROSTER an alle.
	mode, _ := Encode(ModePayload{Mode: ModeEdit})
	send(t, b.control, &frame.Frame{Type: frame.Mode, Payload: mode})
	granted := recvType(t, b.frames, frame.Mode)
	var g ModePayload
	_ = DecodePayload(granted.Payload, &g)
	if g.Mode != ModeEdit {
		t.Fatalf("granted %+v", g)
	}
	roster := recvType(t, a.frames, frame.Roster)
	var r RosterPayload
	_ = DecodePayload(roster.Payload, &r)
	for _, m := range r.Members {
		if m.ID == b.id && (m.Mode != ModeEdit || m.Rank != 1) {
			t.Fatalf("roster %+v", r)
		}
	}
}

func TestOpGoesToTheMasterOnlyAndCommitToEveryoneElse(t *testing.T) {
	_, server := newServer(t)
	a := join(t, server, "anna", "doc-2", "wordcraft", "1", ModeEdit)
	b := join(t, server, "ben", "doc-2", "wordcraft", "1", ModeEdit)
	c := join(t, server, "cem", "doc-2", "wordcraft", "1", ModeReadOnly)
	send(t, b.control, &frame.Frame{Type: frame.Op, Base: 0, Local: 7, Payload: []byte("insert")})
	op := recvType(t, a.frames, frame.Op)
	if op.Sender != b.id || op.Local != 7 || string(op.Payload) != "insert" {
		t.Fatalf("op at the master: %+v", op)
	}
	nothing(t, c.frames, frame.Op, 200*time.Millisecond)
	// Der Leser darf nichts vorschlagen.
	send(t, c.control, &frame.Frame{Type: frame.Op, Local: 1, Payload: []byte("nope")})
	nothing(t, a.frames, frame.Op, 200*time.Millisecond)
	// COMMIT vom Master an alle anderen, mit seq.
	send(t, a.control, &frame.Frame{Type: frame.Commit, Seq: 1, Sender: b.id, Local: 7, Payload: []byte("insert")})
	for _, m := range []*client{b, c} {
		commit := recvType(t, m.frames, frame.Commit)
		if commit.Seq != 1 || commit.Sender != b.id || string(commit.Payload) != "insert" {
			t.Fatalf("commit: %+v", commit)
		}
	}
	// NAK vom Master nur an den Adressaten (`sender` nennt ihn).
	send(t, a.control, &frame.Frame{Type: frame.Nak, Sender: b.id, Local: 8, Payload: []byte("stale")})
	nak := recvType(t, b.frames, frame.Nak)
	if nak.Local != 8 {
		t.Fatalf("nak %+v", nak)
	}
	nothing(t, c.frames, frame.Nak, 200*time.Millisecond)
	// Ein COMMIT von jemand anderem als dem Master geht nirgends hin.
	send(t, b.control, &frame.Frame{Type: frame.Commit, Seq: 2, Payload: []byte("forged")})
	nothing(t, c.frames, frame.Commit, 200*time.Millisecond)
	// Ein zu großer Rahmen wird verworfen.
	send(t, b.control, &frame.Frame{Type: frame.Op, Local: 9, Payload: bytes.Repeat([]byte("x"), frame.MaxControlPayload+1)})
	nothing(t, a.frames, frame.Op, 200*time.Millisecond)
	// PRESENCE an alle anderen.
	send(t, c.control, &frame.Frame{Type: frame.Presence, Payload: []byte("cursor")})
	if p := recvType(t, a.frames, frame.Presence); p.Sender != c.id {
		t.Fatalf("presence %+v", p)
	}
	recvType(t, b.frames, frame.Presence)
}

func TestAnotherAppVersionOnlyReads(t *testing.T) {
	_, server := newServer(t)
	a := join(t, server, "anna", "doc-3", "wordcraft", "1", ModeEdit)
	b := join(t, server, "ben", "doc-3", "wordcraft", "2", ModeEdit)
	if b.ticket.MaxMode != ModeReadOnly || b.ticket.Reason != "appVersion" || b.ticket.AppVersion != "1" {
		t.Fatalf("the ticket already says read-only: %+v", b.ticket)
	}
	if b.welcome.Mode != ModeReadOnly || b.welcome.Reason != "appVersion" || b.welcome.Master != a.id {
		t.Fatalf("welcome: %+v", b.welcome)
	}
	mode, _ := Encode(ModePayload{Mode: ModeEdit})
	send(t, b.control, &frame.Frame{Type: frame.Mode, Payload: mode})
	var g ModePayload
	_ = DecodePayload(recvType(t, b.frames, frame.Mode).Payload, &g)
	if g.Mode != ModeReadOnly || g.Reason != "rights" {
		t.Fatalf("switching is refused: %+v", g)
	}
}

func TestMasterLeavesAndTheNextRankTakesOver(t *testing.T) {
	hub, server := newServer(t)
	a := join(t, server, "anna", "doc-4", "wordcraft", "1", ModeEdit)
	b := join(t, server, "ben", "doc-4", "wordcraft", "1", ModeEdit)
	c := join(t, server, "cem", "doc-4", "wordcraft", "1", ModeEdit)
	send(t, a.control, &frame.Frame{Type: frame.Commit, Seq: 5, Payload: []byte("x")})
	recvType(t, b.frames, frame.Commit)
	recvType(t, c.frames, frame.Commit)
	send(t, a.control, &frame.Frame{Type: frame.Bye})
	var mb, mc MasterPayload
	fb := recvType(t, b.frames, frame.Master)
	_ = DecodePayload(fb.Payload, &mb)
	fc := recvType(t, c.frames, frame.Master)
	_ = DecodePayload(fc.Payload, &mc)
	if mb.Master != b.id || mc.Master != b.id {
		t.Fatalf("the lowest rank becomes master: %+v %+v", mb, mc)
	}
	if fb.Seq != 5 || fc.Seq != 5 {
		t.Fatalf("MASTER carries the last seq: %d %d", fb.Seq, fc.Seq)
	}
	if mb.Lock == "" || mc.Lock != "" {
		t.Fatalf("only the new master gets the lock: %q %q", mb.Lock, mc.Lock)
	}
	// Der neue Master bekommt die OPs.
	send(t, c.control, &frame.Frame{Type: frame.Op, Local: 1, Payload: []byte("again")})
	if op := recvType(t, b.frames, frame.Op); op.Sender != c.id {
		t.Fatalf("op %+v", op)
	}
	send(t, b.control, &frame.Frame{Type: frame.Bye})
	send(t, c.control, &frame.Frame{Type: frame.Bye})
	deadline := time.Now().Add(2 * time.Second)
	for hub.Sessions() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if hub.Sessions() != 0 {
		t.Fatal("the session ends with the last participant")
	}
}

func TestBulkPartsReachTheOthersAndLateComersAskTheBuffer(t *testing.T) {
	_, server := newServer(t)
	a := join(t, server, "anna", "doc-5", "wordcraft", "1", ModeEdit)
	b := join(t, server, "ben", "doc-5", "wordcraft", "1", ModeEdit)
	a.openBulk()
	b.openBulk()
	var hash [HashSize]byte
	copy(hash[:], "0123456789abcdef0123456789abcdef")
	part1 := &BlobPart{Hash: hash, Offset: 0, Data: []byte("hello ")}
	part2 := &BlobPart{Hash: hash, Offset: 6, Data: []byte("world")}
	send(t, b.bulk, &frame.Frame{Type: frame.BlobPart, Payload: EncodeBlobPart(part1)})
	send(t, b.bulk, &frame.Frame{Type: frame.BlobPart, Payload: EncodeBlobPart(part2)})
	send(t, b.bulk, &frame.Frame{Type: frame.BlobDone, Payload: hash[:]})
	for _, want := range []string{"hello ", "world"} {
		got, err := DecodeBlobPart(recvType(t, a.bulkIn, frame.BlobPart).Payload)
		if err != nil || string(got.Data) != want {
			t.Fatalf("part at the master: %v %+v", err, got)
		}
	}
	recvType(t, a.bulkIn, frame.BlobDone)
	// Der Steuerkanal bleibt davon unberührt.
	nothing(t, a.frames, frame.BlobPart, 100*time.Millisecond)
	// Ein Nachzügler fordert den Blob an und bekommt ihn aus dem Puffer.
	c := join(t, server, "cem", "doc-5", "wordcraft", "1", ModeReadOnly)
	c.openBulk()
	send(t, c.bulk, &frame.Frame{Type: frame.BlobReq, Payload: hash[:]})
	var got []byte
	for i := 0; i < 2; i++ {
		part, err := DecodeBlobPart(recvType(t, c.bulkIn, frame.BlobPart).Payload)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, part.Data...)
	}
	recvType(t, c.bulkIn, frame.BlobDone)
	if string(got) != "hello world" {
		t.Fatalf("buffer answered %q", got)
	}
	// Ein unbekannter Blob geht als Anfrage zum Master.
	var other [HashSize]byte
	copy(other[:], "ffffffffffffffffffffffffffffffff")
	send(t, c.bulk, &frame.Frame{Type: frame.BlobReq, Payload: other[:]})
	if req := recvType(t, a.bulkIn, frame.BlobReq); req.Sender != c.id {
		t.Fatalf("request forwarded to the master: %+v", req)
	}
}

func TestTicketsAreSingleUseAndNeedAuthorization(t *testing.T) {
	_, server := newServer(t)
	req, _ := http.NewRequest("POST", server.URL+"/signal/ticket", strings.NewReader(`{"doc":"d","app":"a","appVersion":"1","mode":1}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("without Authorization: %d", resp.StatusCode)
	}
	a := join(t, server, "anna", "doc-6", "wordcraft", "1", ModeEdit)
	// Dasselbe Ticket noch einmal: die Verbindung wird abgewiesen.
	ws := dial(t, server, "/signal")
	payload, _ := Encode(HelloPayload{Ticket: a.ticket.Ticket, App: "wordcraft", AppVersion: "1", Mode: ModeEdit})
	send(t, ws, &frame.Frame{Type: frame.Hello, Payload: payload})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := ws.Read(ctx); err == nil {
		t.Fatal("a used ticket must be refused")
	}
	// Ein Rahmen, der kein HELLO ist, schließt die Verbindung.
	ws2 := dial(t, server, "/signal")
	send(t, ws2, &frame.Frame{Type: frame.Ping})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, _, err := ws2.Read(ctx2); err == nil {
		t.Fatal("HELLO must come first")
	}
}
