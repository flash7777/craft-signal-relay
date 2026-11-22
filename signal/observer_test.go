package signal

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"forge.mux.nu/xworx/craft-signal-relay/frame"
)

// hiddenServer ist ein Relais, auf dem nur recorder verdeckt beitreten darf und root die Betriebsrolle hat.
func hiddenServer(t *testing.T) (*Hub, *httptest.Server) {
	t.Helper()
	hub := New(Options{Dev: true, BlobBudget: 1 << 20, Hidden: func(user string) bool { return user == "recorder" }, Operator: func(user string) bool { return user == "root" }})
	mux := http.NewServeMux()
	hub.Routes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return hub, server
}

// hiddenTicket fragt ein verdecktes Ticket an und nennt den Status.
func hiddenTicket(t *testing.T, server *httptest.Server, user, doc string) (int, TicketResponse) {
	t.Helper()
	body, _ := json.Marshal(TicketRequest{Doc: doc, App: "craft-recorder", AppVersion: "1", Mode: ModeEdit, Name: user, Hidden: true})
	req, _ := http.NewRequest("POST", server.URL+"/signal/ticket", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-"+user)
	req.Header.Set("X-Signal-User", user)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var ticket TicketResponse
	if resp.StatusCode == 200 {
		if err := json.NewDecoder(resp.Body).Decode(&ticket); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, ticket
}

// observe tritt verdeckt bei: Ticket, HELLO, WELCOME.
func observe(t *testing.T, server *httptest.Server, user, doc string) *client {
	t.Helper()
	c := &client{t: t, server: server, user: user, frames: make(chan *frame.Frame, 256), bulkIn: make(chan *frame.Frame, 256)}
	status, ticket := hiddenTicket(t, server, user, doc)
	if status != 200 {
		t.Fatalf("hidden ticket: %d", status)
	}
	c.ticket = ticket
	c.control = dial(t, server, "/signal")
	go pump(c.control, c.frames)
	payload, _ := Encode(HelloPayload{Ticket: ticket.Ticket, App: "craft-recorder", AppVersion: "1", Mode: ModeEdit})
	send(t, c.control, &frame.Frame{Type: frame.Hello, Payload: payload})
	w := recvType(t, c.frames, frame.Welcome)
	if err := DecodePayload(w.Payload, &c.welcome); err != nil {
		t.Fatal(err)
	}
	c.id, c.session = w.Sender, w.Session
	return c
}

func drain(in chan *frame.Frame) {
	for {
		select {
		case <-in:
		default:
			return
		}
	}
}

func TestAHiddenTicketNeedsTheRoleAndTheSwitch(t *testing.T) {
	_, server := hiddenServer(t)
	if status, _ := hiddenTicket(t, server, "anna", "doc-h"); status != http.StatusForbidden {
		t.Fatalf("without the role: %d", status)
	}
	if status, ticket := hiddenTicket(t, server, "recorder", "doc-h"); status != 200 || ticket.MaxMode != ModeReadOnly {
		t.Fatalf("with the role: %d %+v", status, ticket)
	}
	// Abgeschaltet auf der Instanz: auch mit jedem Namen nicht.
	_, off := newServer(t)
	if status, _ := hiddenTicket(t, off, "recorder", "doc-h"); status != http.StatusForbidden {
		t.Fatalf("switched off: %d", status)
	}
}

func TestAnObserverHearsWhatAReaderHearsAndNobodySeesHim(t *testing.T) {
	hub, server := hiddenServer(t)
	a := join(t, server, "anna", "doc-h", "wordcraft", "1", ModeEdit)
	b := join(t, server, "ben", "doc-h", "wordcraft", "1", ModeEdit)
	time.Sleep(100 * time.Millisecond)
	drain(a.frames)
	drain(b.frames)

	r := observe(t, server, "recorder", "doc-h")
	// Er weiß, wer Master ist und wer da ist; er selbst liest nur und hat keinen Rang.
	if r.welcome.Master != a.id || r.welcome.Mode != ModeReadOnly || r.welcome.Rank != NoRank {
		t.Fatalf("welcome: %+v", r.welcome)
	}
	var roster RosterPayload
	if err := DecodePayload(recvType(t, r.frames, frame.Roster).Payload, &roster); err != nil || len(roster.Members) != 2 {
		t.Fatalf("roster for the observer: %v %+v", err, roster)
	}
	// Die anderen erfahren von seinem Beitritt nichts.
	nothing(t, a.frames, frame.Roster, 200*time.Millisecond)
	nothing(t, b.frames, frame.Roster, 100*time.Millisecond)

	// Was der Master bestätigt, kommt bei ihm an; auch die Bytes auf dem Bulk-Kanal.
	send(t, a.control, &frame.Frame{Type: frame.Commit, Seq: 1, Sender: b.id, Local: 3, Payload: []byte("insert")})
	if commit := recvType(t, r.frames, frame.Commit); commit.Seq != 1 || string(commit.Payload) != "insert" {
		t.Fatalf("commit at the observer: %+v", commit)
	}
	// Zum Debuggen auch, was ein Leser nicht bekommt: Vorschläge und Ablehnungen.
	send(t, b.control, &frame.Frame{Type: frame.Op, Local: 4, Payload: []byte("proposal")})
	if op := recvType(t, r.frames, frame.Op); op.Sender != b.id || op.Local != 4 || string(op.Payload) != "proposal" {
		t.Fatalf("op at the observer: %+v", op)
	}
	recvType(t, a.frames, frame.Op)
	send(t, a.control, &frame.Frame{Type: frame.Nak, Sender: b.id, Local: 4, Payload: []byte("stale")})
	if nak := recvType(t, r.frames, frame.Nak); nak.Sender != b.id || nak.Local != 4 || string(nak.Payload) != "stale" {
		t.Fatalf("nak at the observer: %+v", nak)
	}
	recvType(t, b.frames, frame.Nak)
	b.openBulk()
	r.openBulk()
	var hash [HashSize]byte
	copy(hash[:], "0123456789abcdef0123456789abcdef")
	send(t, b.bulk, &frame.Frame{Type: frame.BlobPart, Payload: EncodeBlobPart(&BlobPart{Hash: hash, Offset: 0, Data: []byte("picture")})})
	send(t, b.bulk, &frame.Frame{Type: frame.BlobDone, Payload: hash[:]})
	if part, err := DecodeBlobPart(recvType(t, r.bulkIn, frame.BlobPart).Payload); err != nil || string(part.Data) != "picture" {
		t.Fatalf("blob at the observer: %v %+v", err, part)
	}
	recvType(t, r.bulkIn, frame.BlobDone)

	// Er selbst schlägt nichts vor, zeigt keine Präsenz, wechselt den Modus nicht und bringt keine Bytes ein.
	send(t, r.control, &frame.Frame{Type: frame.Op, Local: 1, Payload: []byte("nope")})
	send(t, r.control, &frame.Frame{Type: frame.Presence, Payload: []byte("cursor")})
	modePayload, _ := Encode(ModePayload{Mode: ModeEdit})
	send(t, r.control, &frame.Frame{Type: frame.Mode, Payload: modePayload})
	send(t, r.bulk, &frame.Frame{Type: frame.BlobPart, Payload: EncodeBlobPart(&BlobPart{Hash: hash, Offset: 0, Data: []byte("forged")})})
	nothing(t, a.frames, frame.Op, 200*time.Millisecond)
	nothing(t, a.frames, frame.Presence, 50*time.Millisecond)
	nothing(t, a.frames, frame.Roster, 50*time.Millisecond)
	nothing(t, r.frames, frame.Mode, 50*time.Millisecond)

	// Die Auskunft: Teilnehmer sehen zwei, die Betriebsrolle sieht ihn.
	_, annas := askSessions(t, server, "anna", "doc-h")
	if annas.Sessions[0].Participants != 2 || len(annas.Sessions[0].Members) != 2 || annas.Sessions[0].Observers != nil {
		t.Fatalf("anna: %+v", annas.Sessions[0])
	}
	_, roots := askSessions(t, server, "root", "doc-h")
	if roots.Sessions[0].Participants != 2 || len(roots.Sessions[0].Observers) != 1 || roots.Sessions[0].Observers[0].Name != "recorder" {
		t.Fatalf("root: %+v", roots.Sessions[0])
	}
	// Als Beobachter gilt er nicht als Teilnehmer: die eigene Auskunft ist die eines Lesers.
	_, own := askSessions(t, server, "recorder", "doc-h")
	if own.Sessions[0].Detail {
		t.Fatalf("recorder: %+v", own.Sessions[0])
	}

	// Geht der Master, erfährt er den neuen; gehen alle, endet die Sitzung auch für ihn.
	_ = a.control.Close(websocket.StatusNormalClosure, "bye")
	var master MasterPayload
	if err := DecodePayload(recvType(t, r.frames, frame.Master).Payload, &master); err != nil || master.Master != b.id || master.Lock != "" {
		t.Fatalf("master for the observer: %v %+v", err, master)
	}
	_ = b.control.Close(websocket.StatusNormalClosure, "bye")
	deadline := time.After(3 * time.Second)
	for closed := false; !closed; {
		select {
		case _, ok := <-r.frames:
			closed = !ok
		case <-deadline:
			t.Fatal("the observer keeps the session alive")
		}
	}
	if hub.Sessions() != 0 {
		t.Fatalf("sessions left: %d", hub.Sessions())
	}
}

func TestAnObserverDoesNotOpenASession(t *testing.T) {
	hub, server := hiddenServer(t)
	status, ticket := hiddenTicket(t, server, "recorder", "doc-empty")
	if status != 200 {
		t.Fatalf("ticket: %d", status)
	}
	control := dial(t, server, "/signal")
	frames := make(chan *frame.Frame, 16)
	go pump(control, frames)
	payload, _ := Encode(HelloPayload{Ticket: ticket.Ticket, App: "craft-recorder", AppVersion: "1", Mode: ModeReadOnly})
	send(t, control, &frame.Frame{Type: frame.Hello, Payload: payload})
	select {
	case f, ok := <-frames:
		if ok {
			t.Fatalf("unexpected %s", f.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("not refused")
	}
	if hub.Sessions() != 0 {
		t.Fatalf("an observer opened a session: %d", hub.Sessions())
	}
}
