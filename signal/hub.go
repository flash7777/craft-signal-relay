package signal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"forge.mux.nu/xworx/craft-signal-relay/frame"
)

// TicketTTL ist, wie lange ein Ticket auf seinen HELLO wartet.
const TicketTTL = 60 * time.Second

// HelloTimeout ist, wie lange eine neue Verbindung auf ihren HELLO bekommt.
const HelloTimeout = 10 * time.Second

// sendQueue ist die Zahl der Rahmen, die je Verbindung auf das Senden warten dürfen.
const sendQueue = 512

// writeTimeout ist, wie lange ein Rahmen auf einen langsamen Empfänger wartet, bevor die Verbindung fällt.
const writeTimeout = 5 * time.Second

// Options stellt das Relais ein.
type Options struct {
	// Dev nimmt jede nicht leere Authorization als Nutzer (Testbench, offline). Ohne Dev prüft Authorize.
	Dev bool
	// Authorize prüft die Anmeldung einer Ticketanfrage und nennt den Nutzer; nil ohne Dev heißt: alles abgelehnt.
	Authorize func(r *http.Request) (user string, err error)
	// Rights sagt, was der Nutzer an dem Dokument darf (OpenCore: Stat mit seinen Rechten): den höchsten Modus
	// (lesen oder bearbeiten) und ob er es überhaupt sehen darf. Gefragt von der Ticketausgabe und der
	// Sitzungsauskunft, mit der Anfrage, an der die Anmeldung hängt. nil heißt im Dev-Modus: alles erlaubt; sonst:
	// nichts.
	Rights func(r *http.Request, user, doc string) (maxMode uint8, ok bool)
	// DisplayName nennt den Anzeigenamen des Angemeldeten; er geht vor dem Namen, den die Anfrage selbst nennt.
	// nil: der Name aus der Anfrage gilt (Dev).
	DisplayName func(r *http.Request) string
	// Operator sagt, ob der Nutzer die Betriebsrolle hat: er sieht in der Sitzungsauskunft alle Sitzungen ganz.
	// nil heißt: niemand.
	Operator func(user string) bool
	// Hidden sagt, ob der Nutzer verdeckt beitreten darf (eigene Rolle; SIGNAL.md „Recorder“): die anderen sehen
	// ihn nicht. nil heißt: auf dieser Instanz abgeschaltet.
	Hidden func(user string) bool
	// Lock vergibt die Sperrkennung für ein Dokument (OpenCore: CS3 SetLock); nil erzeugt eine zufällige.
	Lock func(doc, user string) (string, error)
	// Unlock gibt die Sperre frei, wenn die Sitzung endet.
	Unlock func(doc, token string)
	// BlobBudget ist der Sitzungspuffer für Blobs in Byte.
	BlobBudget int
	Log        *slog.Logger
}

// TicketRequest ist die Anfrage an POST /signal/ticket.
type TicketRequest struct {
	Doc        string `json:"doc"`
	App        string `json:"app"`
	AppVersion string `json:"appVersion"`
	// Mode ist der gewünschte Höchstmodus (0 lesen, 1 bearbeiten).
	Mode uint8  `json:"mode"`
	Name string `json:"name,omitempty"`
	// Hidden verlangt ein verdecktes Ticket: nur lesen, für die anderen unsichtbar. Braucht die Rolle dafür.
	Hidden bool `json:"hidden,omitempty"`
}

// TicketResponse ist die Antwort: das Ticket und, wenn zum Dokument schon eine Sitzung läuft, deren Typ.
type TicketResponse struct {
	Ticket     string `json:"ticket"`
	Session    uint32 `json:"session,omitempty"`
	App        string `json:"app,omitempty"`
	AppVersion string `json:"appVersion,omitempty"`
	// MaxMode ist, was das Ticket höchstens erlaubt (Rechte und Sitzungstyp).
	MaxMode uint8  `json:"maxMode"`
	Reason  string `json:"reason,omitempty"`
}

type ticket struct {
	id         string
	doc        string
	user       string
	name       string
	app        string
	appVersion string
	maxMode    uint8
	hidden     bool
	expires    time.Time
	// participant ist gesetzt, sobald der Steuerkanal angemeldet ist; der Bulk-Kanal hängt sich daran.
	participant *Participant
}

// Participant ist ein Teilnehmer einer Sitzung.
type Participant struct {
	ID      uint16
	Name    string
	User    string
	Mode    uint8
	Rank    uint16
	maxMode uint8
	// hidden: ein verdeckter Beobachter. Er steht nicht unter den Teilnehmern, sondern unter Session.observers.
	hidden  bool
	control *conn
	bulk    *conn
}

// Session ist eine Sitzung zu einem Dokument, typisiert durch App und App-Version des Masters.
type Session struct {
	ID         uint32
	Doc        string
	App        string
	AppVersion string
	Seq        uint32
	lock       string
	nextID     uint16
	nextRank   uint16
	members    map[uint16]*Participant
	// observers sind die verdeckten Beobachter: sie bekommen, was ein Leser bekommt, stehen aber in keinem ROSTER,
	// zählen nicht mit, haben keinen Rang und halten die Sitzung nicht am Leben.
	observers map[uint16]*Participant
	master    *Participant
	blobs     *blobStore
}

// Hub ist das Relais.
type Hub struct {
	opts     Options
	log      *slog.Logger
	mu       sync.Mutex
	sessions map[string]*Session
	tickets  map[string]*ticket
	nextSess uint32
}

// New macht ein Relais.
func New(opts Options) *Hub {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.BlobBudget <= 0 {
		opts.BlobBudget = DefaultBlobBudget
	}
	return &Hub{opts: opts, log: opts.Log, sessions: map[string]*Session{}, tickets: map[string]*ticket{}}
}

// Routes hängt die Endpunkte an einen Multiplexer: POST /signal/ticket, GET /signal, GET /signal/bulk,
// GET /signal/sessions.
func (h *Hub) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /signal/ticket", h.handleTicket)
	mux.HandleFunc("GET /signal", h.handleControl)
	mux.HandleFunc("GET /signal/bulk", h.handleBulk)
	mux.HandleFunc("GET /signal/sessions", h.handleSessions)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(b[:])
}

func (h *Hub) authorize(r *http.Request) (string, error) {
	if h.opts.Authorize != nil {
		return h.opts.Authorize(r)
	}
	if !h.opts.Dev {
		return "", errors.New("no authorizer")
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" {
		return "", errors.New("missing Authorization")
	}
	// Dev: der Nutzer ist der Name hinter dem Token, damit zwei Testclients zwei Nutzer sind.
	if name := strings.TrimSpace(r.Header.Get("X-Signal-User")); name != "" {
		return name, nil
	}
	parts := strings.Fields(auth)
	if len(parts) == 2 {
		return parts[1], nil
	}
	return auth, nil
}

// rights fragt den Haken; ohne ihn darf im Dev-Modus jeder alles und sonst niemand etwas.
func (h *Hub) rights(r *http.Request, user, doc string) (uint8, bool) {
	if h.opts.Rights != nil {
		return h.opts.Rights(r, user, doc)
	}
	return ModeEdit, h.opts.Dev
}

// handleTicket stellt ein Ticket für genau eine Sitzung aus und nennt den Typ einer laufenden Sitzung.
func (h *Hub) handleTicket(w http.ResponseWriter, r *http.Request) {
	user, err := h.authorize(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req TicketRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	req.Doc, req.App, req.AppVersion = strings.TrimSpace(req.Doc), strings.TrimSpace(req.App), strings.TrimSpace(req.AppVersion)
	if req.Doc == "" || req.App == "" || req.AppVersion == "" {
		http.Error(w, "doc, app and appVersion are required", http.StatusBadRequest)
		return
	}
	if req.Mode > ModeEdit {
		req.Mode = ModeEdit
	}
	// Was der Nutzer an dem Dokument darf, begrenzt das Ticket; wer es nicht lesen darf, bekommt keines. Die Antwort
	// ist dieselbe wie für ein Dokument, das es nicht gibt.
	allowed, ok := h.rights(r, user, req.Doc)
	if !ok {
		http.Error(w, "no access to the document", http.StatusForbidden)
		return
	}
	if req.Mode > allowed {
		req.Mode = allowed
	}
	if h.opts.DisplayName != nil {
		if name := strings.TrimSpace(h.opts.DisplayName(r)); name != "" {
			req.Name = name
		}
	}
	if req.Hidden {
		if h.opts.Hidden == nil || !h.opts.Hidden(user) {
			http.Error(w, "hidden tickets are not available", http.StatusForbidden)
			return
		}
		req.Mode = ModeReadOnly
	}
	t := &ticket{id: randomID(), doc: req.Doc, user: user, name: req.Name, app: req.App, appVersion: req.AppVersion, maxMode: req.Mode, hidden: req.Hidden, expires: time.Now().Add(TicketTTL)}
	resp := TicketResponse{Ticket: t.id, MaxMode: t.maxMode}
	h.mu.Lock()
	h.sweepTicketsLocked()
	if s, ok := h.sessions[req.Doc]; ok {
		resp.Session, resp.App, resp.AppVersion = s.ID, s.App, s.AppVersion
		if s.App != req.App || s.AppVersion != req.AppVersion {
			t.maxMode = ModeReadOnly
			resp.MaxMode, resp.Reason = ModeReadOnly, "appVersion"
		}
	}
	h.tickets[t.id] = t
	h.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Hub) sweepTicketsLocked() {
	now := time.Now()
	for id, t := range h.tickets {
		if now.After(t.expires) {
			delete(h.tickets, id)
		}
	}
}

// conn ist eine WebSocket-Verbindung mit eigenem Schreiber: Senden blockiert die Sitzung nie.
type conn struct {
	ws     *websocket.Conn
	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func newConn(ws *websocket.Conn) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{ws: ws, send: make(chan []byte, sendQueue), ctx: ctx, cancel: cancel}
	go c.writer()
	return c
}

func (c *conn) writer() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case data := <-c.send:
			ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.ws.Write(ctx, websocket.MessageBinary, data)
			cancel()
			if err != nil {
				c.close(websocket.StatusPolicyViolation, "write failed")
				return
			}
		}
	}
}

// push stellt einen Rahmen in die Warteschlange; ein voller Empfänger verliert ihn (dropable) oder die Verbindung.
func (c *conn) push(data []byte, dropable bool) {
	select {
	case c.send <- data:
	default:
		if !dropable {
			c.close(websocket.StatusPolicyViolation, "too slow")
		}
	}
}

func (c *conn) sendFrame(f *frame.Frame, dropable bool) {
	data, err := f.Encode()
	if err != nil {
		return
	}
	c.push(data, dropable)
}

// close beendet die Verbindung sofort, ohne Schließ-Handschlag: der würde auf die Gegenseite warten, und das
// Relais hält dabei womöglich die Sitzung (leave).
func (c *conn) close(code websocket.StatusCode, reason string) {
	c.once.Do(func() {
		c.cancel()
		_ = code
		_ = reason
		_ = c.ws.CloseNow()
	})
}

func (h *Hub) accept(w http.ResponseWriter, r *http.Request, limit int64) (*conn, error) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(limit)
	return newConn(ws), nil
}

// readHello wartet auf den ersten Rahmen, der HELLO sein muss.
func (h *Hub) readHello(c *conn) (*frame.Frame, *HelloPayload, error) {
	ctx, cancel := context.WithTimeout(c.ctx, HelloTimeout)
	defer cancel()
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return nil, nil, err
	}
	f, err := frame.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	if f.Type != frame.Hello {
		return nil, nil, errors.New("first frame is not HELLO")
	}
	var hello HelloPayload
	if err := DecodePayload(f.Payload, &hello); err != nil {
		return nil, nil, err
	}
	return f, &hello, nil
}

// takeTicket holt ein gültiges Ticket; für den Bulk-Kanal bleibt es bis zum Ablauf, für den Steuerkanal wird es verbraucht.
func (h *Hub) takeTicket(id string, bulk bool) (*ticket, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sweepTicketsLocked()
	t, ok := h.tickets[id]
	if !ok {
		return nil, errors.New("unknown or expired ticket")
	}
	if bulk {
		if t.participant == nil {
			return nil, errors.New("bulk before control")
		}
		return t, nil
	}
	if t.participant != nil {
		return nil, errors.New("ticket already used")
	}
	return t, nil
}

func (h *Hub) handleControl(w http.ResponseWriter, r *http.Request) {
	c, err := h.accept(w, r, frame.HeaderSize+frame.MaxControlPayload+256)
	if err != nil {
		return
	}
	hf, hello, err := h.readHello(c)
	if err != nil {
		h.log.Debug("control: no hello", "err", err)
		c.close(websocket.StatusPolicyViolation, "HELLO expected")
		return
	}
	t, err := h.takeTicket(hello.Ticket, false)
	if err != nil {
		h.log.Debug("control: ticket refused", "err", err)
		c.close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	s, p := h.join(t, hello, hf.Base, c)
	if s == nil {
		c.close(websocket.StatusPolicyViolation, "no session")
		return
	}
	defer h.leave(s, p)
	for {
		_, data, err := c.ws.Read(c.ctx)
		if err != nil {
			return
		}
		f, err := frame.Decode(data)
		if err != nil {
			h.log.Debug("control: bad frame", "err", err)
			continue
		}
		if f.Type == frame.Bye {
			return
		}
		h.handleControlFrame(s, p, f)
	}
}

func (h *Hub) handleBulk(w http.ResponseWriter, r *http.Request) {
	c, err := h.accept(w, r, frame.HeaderSize+frame.MaxBulkPayload+256)
	if err != nil {
		return
	}
	_, hello, err := h.readHello(c)
	if err != nil {
		c.close(websocket.StatusPolicyViolation, "HELLO expected")
		return
	}
	t, err := h.takeTicket(hello.Ticket, true)
	if err != nil {
		c.close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	h.mu.Lock()
	p := t.participant
	s := h.sessions[t.doc]
	if p != nil {
		p.bulk = c
	}
	h.mu.Unlock()
	if p == nil || s == nil {
		c.close(websocket.StatusPolicyViolation, "no session")
		return
	}
	c.sendFrame(&frame.Frame{Type: frame.Welcome, Session: s.ID, Sender: p.ID}, false)
	defer func() {
		h.mu.Lock()
		if p.bulk == c {
			p.bulk = nil
		}
		h.mu.Unlock()
		c.close(websocket.StatusNormalClosure, "bye")
	}()
	for {
		_, data, err := c.ws.Read(c.ctx)
		if err != nil {
			return
		}
		f, err := frame.Decode(data)
		if err != nil {
			continue
		}
		if f.Type == frame.Bye {
			return
		}
		h.handleBulkFrame(s, p, f)
	}
}

// join nimmt einen Teilnehmer in die Sitzung seines Dokuments auf, legt sie an, wenn er der erste Bearbeiter ist,
// und schickt WELCOME an ihn und ROSTER an alle.
func (h *Hub) join(t *ticket, hello *HelloPayload, base uint32, c *conn) (*Session, *Participant) {
	h.mu.Lock()
	defer h.mu.Unlock()
	wanted := hello.Mode
	if wanted > t.maxMode {
		wanted = t.maxMode
	}
	reason := ""
	if wanted < hello.Mode {
		reason = "rights"
	}
	s, exists := h.sessions[t.doc]
	if t.hidden {
		// Ein Beobachter eröffnet keine Sitzung und sperrt nichts.
		if !exists {
			return nil, nil
		}
		return s, h.observeLocked(s, t, base, c)
	}
	if !exists {
		h.nextSess++
		s = &Session{ID: h.nextSess, Doc: t.doc, App: t.app, AppVersion: t.appVersion, members: map[uint16]*Participant{}, observers: map[uint16]*Participant{}, blobs: newBlobStore(h.opts.BlobBudget)}
		h.sessions[t.doc] = s
	}
	if s.App != hello.App || s.AppVersion != hello.AppVersion {
		wanted, reason = ModeReadOnly, "appVersion"
	}
	s.nextID++
	p := &Participant{ID: s.nextID, Name: t.name, User: t.user, Mode: wanted, Rank: NoRank, maxMode: t.maxMode, control: c}
	t.participant = p
	s.members[p.ID] = p
	if wanted == ModeEdit {
		p.Rank = s.nextRank
		s.nextRank++
	}
	welcome := WelcomePayload{Rank: p.Rank, Master: 0, Mode: wanted, App: s.App, AppVersion: s.AppVersion, Reason: reason}
	if s.master == nil && wanted == ModeEdit {
		s.master = p
		s.lock = h.lockFor(s.Doc, p.User)
		welcome.Lock = s.lock
	}
	if s.master != nil {
		welcome.Master = s.master.ID
	}
	payload, _ := Encode(welcome)
	c.sendFrame(&frame.Frame{Type: frame.Welcome, Session: s.ID, Sender: p.ID, Seq: s.Seq, Base: base, Payload: payload}, false)
	h.broadcastRosterLocked(s)
	h.log.Info("join", "session", s.ID, "doc", s.Doc, "participant", p.ID, "user", p.User, "mode", wanted, "rank", p.Rank, "reason", reason)
	return s, p
}

// observeLocked hängt einen verdeckten Beobachter an eine laufende Sitzung: WELCOME an ihn, kein ROSTER an die
// anderen. Er erfährt Master und Teilnehmer wie ein Leser.
func (h *Hub) observeLocked(s *Session, t *ticket, base uint32, c *conn) *Participant {
	s.nextID++
	p := &Participant{ID: s.nextID, Name: t.name, User: t.user, Mode: ModeReadOnly, Rank: NoRank, maxMode: ModeReadOnly, hidden: true, control: c}
	t.participant = p
	s.observers[p.ID] = p
	payload, _ := Encode(WelcomePayload{Rank: NoRank, Master: masterID(s), Mode: ModeReadOnly, App: s.App, AppVersion: s.AppVersion, Reason: "hidden"})
	c.sendFrame(&frame.Frame{Type: frame.Welcome, Session: s.ID, Sender: p.ID, Seq: s.Seq, Base: base, Payload: payload}, false)
	c.sendFrame(&frame.Frame{Type: frame.Roster, Session: s.ID, Seq: s.Seq, Payload: rosterLocked(s)}, false)
	h.log.Info("observe", "session", s.ID, "doc", s.Doc, "participant", p.ID, "user", p.User)
	return p
}

// toObserversLocked reicht einen Rahmen des Steuerkanals an die verdeckten Beobachter.
func toObserversLocked(s *Session, f *frame.Frame, droppable bool) {
	for _, observer := range s.observers {
		observer.control.sendFrame(f, droppable)
	}
}

func (h *Hub) lockFor(doc, user string) string {
	if h.opts.Lock != nil {
		token, err := h.opts.Lock(doc, user)
		if err != nil {
			h.log.Warn("lock failed", "doc", doc, "err", err)
			return ""
		}
		return token
	}
	return "craft-signal-" + randomID()
}

// leave nimmt einen Teilnehmer heraus; geht der Master, rückt der nächste Rang nach (MASTER an alle); geht der
// Letzte, endet die Sitzung und die Sperre wird freigegeben.
func (h *Hub) leave(s *Session, p *Participant) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p.hidden {
		if _, ok := s.observers[p.ID]; ok {
			delete(s.observers, p.ID)
			p.closeAll()
		}
		return
	}
	if _, ok := s.members[p.ID]; !ok {
		return
	}
	delete(s.members, p.ID)
	p.closeAll()
	if len(s.members) == 0 {
		// Beobachter halten die Sitzung nicht: sie endet auch für sie.
		for id, observer := range s.observers {
			delete(s.observers, id)
			observer.closeAll()
		}
		if h.opts.Unlock != nil && s.lock != "" {
			h.opts.Unlock(s.Doc, s.lock)
		}
		if h.sessions[s.Doc] == s {
			delete(h.sessions, s.Doc)
		}
		h.log.Info("session ended", "session", s.ID, "doc", s.Doc)
		return
	}
	if s.master == p {
		s.master = h.electLocked(s)
		for _, m := range s.members {
			payload := MasterPayload{}
			if s.master != nil {
				payload.Master = s.master.ID
				if m == s.master {
					payload.Lock = s.lock
				}
			}
			data, _ := Encode(payload)
			m.control.sendFrame(&frame.Frame{Type: frame.Master, Session: s.ID, Seq: s.Seq, Payload: data}, false)
		}
		announceMasterToObserversLocked(s)
		h.log.Info("master changed", "session", s.ID, "master", masterID(s))
	}
	h.broadcastRosterLocked(s)
}

func (p *Participant) closeAll() {
	p.control.close(websocket.StatusNormalClosure, "bye")
	if p.bulk != nil {
		p.bulk.close(websocket.StatusNormalClosure, "bye")
	}
}

func masterID(s *Session) uint16 {
	if s.master == nil {
		return 0
	}
	return s.master.ID
}

// electLocked nennt den Bearbeiter mit dem niedrigsten Rang.
func (h *Hub) electLocked(s *Session) *Participant {
	var best *Participant
	for _, m := range s.members {
		if m.Mode != ModeEdit {
			continue
		}
		if best == nil || m.Rank < best.Rank {
			best = m
		}
	}
	return best
}

// announceMasterToObserversLocked nennt den Beobachtern den Master, ohne die Sperrkennung.
func announceMasterToObserversLocked(s *Session) {
	data, _ := Encode(MasterPayload{Master: masterID(s)})
	toObserversLocked(s, &frame.Frame{Type: frame.Master, Session: s.ID, Seq: s.Seq, Payload: data}, false)
}

func (h *Hub) broadcastRosterLocked(s *Session) {
	data := rosterLocked(s)
	for _, m := range s.members {
		m.control.sendFrame(&frame.Frame{Type: frame.Roster, Session: s.ID, Seq: s.Seq, Payload: data}, false)
	}
	toObserversLocked(s, &frame.Frame{Type: frame.Roster, Session: s.ID, Seq: s.Seq, Payload: data}, false)
}

// rosterLocked ist die Teilnehmerliste als Nutzlast; Beobachter stehen nicht darin.
func rosterLocked(s *Session) []byte {
	roster := RosterPayload{Master: masterID(s)}
	for _, m := range s.members {
		roster.Members = append(roster.Members, RosterEntry{ID: m.ID, Rank: m.Rank, Mode: m.Mode, Name: m.Name})
	}
	for i := 1; i < len(roster.Members); i++ {
		for j := i; j > 0 && roster.Members[j].ID < roster.Members[j-1].ID; j-- {
			roster.Members[j], roster.Members[j-1] = roster.Members[j-1], roster.Members[j]
		}
	}
	data, _ := Encode(roster)
	return data
}

// handleControlFrame verteilt einen Rahmen des Steuerkanals nach den Regeln aus SIGNAL.md.
func (h *Hub) handleControlFrame(s *Session, p *Participant, f *frame.Frame) {
	if len(f.Payload) > frame.MaxControlPayload {
		h.log.Debug("control: frame too big, dropped", "participant", p.ID, "type", f.Type.String(), "len", len(f.Payload))
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	f.Session = s.ID
	if p.hidden && f.Type != frame.Ping && f.Type != frame.Pong && f.Type != frame.SyncReq {
		// Ein Beobachter hört zu: er schlägt nichts vor, zeigt keine Präsenz und wechselt den Modus nicht.
		return
	}
	switch f.Type {
	case frame.Ping:
		p.control.sendFrame(&frame.Frame{Type: frame.Pong, Session: s.ID, Seq: s.Seq}, true)
	case frame.Pong:
	case frame.Op, frame.SyncReq:
		// Nur zum Master; ein Leser hat nichts vorzuschlagen, der Master schlägt sich nichts vor.
		if f.Type == frame.Op && p.Mode != ModeEdit {
			return
		}
		if s.master == nil || s.master == p {
			return
		}
		f.Sender = p.ID
		s.master.control.sendFrame(f, false)
		if f.Type == frame.Op {
			// Beobachter sind zum Debuggen da: sie bekommen auch die Vorschläge, nicht nur was bestätigt wird.
			toObserversLocked(s, f, false)
		}
	case frame.Commit, frame.Saved:
		if s.master != p {
			return
		}
		if f.Type == frame.Commit && f.Seq > s.Seq {
			s.Seq = f.Seq
		}
		// Im COMMIT nennt `sender` den Urheber des Vorschlags (der Master setzt ihn), damit der
		// Absender sein eigenes COMMIT an `sender` + `local` erkennt; nur SAVED trägt den Master.
		if f.Type == frame.Saved {
			f.Sender = p.ID
		}
		for _, m := range s.members {
			if m != p {
				m.control.sendFrame(f, false)
			}
		}
		toObserversLocked(s, f, false)
	case frame.Nak, frame.Sync:
		// Vom Master an einen Adressaten: `sender` nennt ihn (Kopf hat kein Zielfeld).
		if s.master != p {
			return
		}
		if target, ok := s.members[f.Sender]; ok && target != p {
			target.control.sendFrame(f, false)
		} else if observer, ok := s.observers[f.Sender]; ok && f.Type == frame.Sync {
			// Der Stand, den ein Beobachter angefordert hat.
			observer.control.sendFrame(f, false)
		}
		if f.Type == frame.Nak {
			// Auch die Ablehnungen (wie die Vorschläge): `sender` nennt, wessen Vorschlag abgelehnt wurde.
			toObserversLocked(s, f, false)
		}
	case frame.Presence:
		f.Sender = p.ID
		for _, m := range s.members {
			if m != p {
				m.control.sendFrame(f, true)
			}
		}
		toObserversLocked(s, f, true)
	case frame.Mode:
		var wanted ModePayload
		if err := DecodePayload(f.Payload, &wanted); err != nil {
			return
		}
		granted := h.changeModeLocked(s, p, wanted.Mode)
		data, _ := Encode(granted)
		p.control.sendFrame(&frame.Frame{Type: frame.Mode, Session: s.ID, Sender: p.ID, Seq: s.Seq, Payload: data}, false)
		h.broadcastRosterLocked(s)
	default:
		h.log.Debug("control: type not for this channel, dropped", "participant", p.ID, "type", f.Type.String())
	}
}

// changeModeLocked schaltet zwischen Lesen und Kollaboration; Bearbeiten nur im Rahmen des Tickets und des
// Sitzungstyps (den hat das Relais beim HELLO schon auf das Ticket angewendet).
func (h *Hub) changeModeLocked(s *Session, p *Participant, wanted uint8) ModePayload {
	if wanted > ModeEdit {
		wanted = ModeEdit
	}
	if wanted == ModeEdit {
		if p.maxMode < ModeEdit {
			return ModePayload{Mode: p.Mode, Reason: "rights"}
		}
		if p.Mode != ModeEdit {
			p.Mode = ModeEdit
			p.Rank = s.nextRank
			s.nextRank++
			if s.master == nil {
				s.master = p
				s.lock = h.lockFor(s.Doc, p.User)
				data, _ := Encode(MasterPayload{Master: p.ID, Lock: s.lock})
				for _, m := range s.members {
					if m == p {
						m.control.sendFrame(&frame.Frame{Type: frame.Master, Session: s.ID, Seq: s.Seq, Payload: data}, false)
					} else {
						other, _ := Encode(MasterPayload{Master: p.ID})
						m.control.sendFrame(&frame.Frame{Type: frame.Master, Session: s.ID, Seq: s.Seq, Payload: other}, false)
					}
				}
				announceMasterToObserversLocked(s)
			}
		}
		return ModePayload{Mode: ModeEdit}
	}
	if p.Mode == ModeEdit {
		p.Mode = ModeReadOnly
		p.Rank = NoRank
		if s.master == p {
			s.master = h.electLocked(s)
			for _, m := range s.members {
				payload := MasterPayload{Master: masterID(s)}
				if s.master != nil && m == s.master {
					payload.Lock = s.lock
				}
				data, _ := Encode(payload)
				m.control.sendFrame(&frame.Frame{Type: frame.Master, Session: s.ID, Seq: s.Seq, Payload: data}, false)
			}
			announceMasterToObserversLocked(s)
		}
	}
	return ModePayload{Mode: ModeReadOnly}
}

// handleBulkFrame verteilt die Bytes eines Blobs: Teile an alle anderen, den Master zuerst, und in den
// Sitzungspuffer; eine Nachforderung bedient der Puffer oder der Master.
func (h *Hub) handleBulkFrame(s *Session, p *Participant, f *frame.Frame) {
	if len(f.Payload) > frame.MaxBulkPayload {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	f.Session = s.ID
	f.Sender = p.ID
	if p.hidden && (f.Type == frame.BlobPart || f.Type == frame.BlobDone) {
		// Ein Beobachter bringt nichts ein.
		return
	}
	switch f.Type {
	case frame.BlobPart:
		part, err := DecodeBlobPart(f.Payload)
		if err != nil {
			return
		}
		s.blobs.put(part)
		h.fanOutBulkLocked(s, p, f)
	case frame.BlobDone:
		req, err := DecodeBlobRequest(f.Payload)
		if err != nil {
			return
		}
		s.blobs.finish(req.Hash)
		h.fanOutBulkLocked(s, p, f)
	case frame.BlobReq:
		req, err := DecodeBlobRequest(f.Payload)
		if err != nil || p.bulk == nil {
			return
		}
		parts, done := s.blobs.parts(req.Hash)
		if len(parts) == 0 || !done {
			// Nicht (vollständig) im Puffer: der Master hat alles.
			if s.master != nil && s.master != p && s.master.bulk != nil {
				s.master.bulk.sendFrame(f, false)
			}
			return
		}
		for _, part := range parts {
			p.bulk.sendFrame(&frame.Frame{Type: frame.BlobPart, Session: s.ID, Sender: 0, Payload: EncodeBlobPart(part)}, false)
		}
		p.bulk.sendFrame(&frame.Frame{Type: frame.BlobDone, Session: s.ID, Sender: 0, Payload: req.Hash[:]}, false)
	case frame.Ping:
		p.bulk.sendFrame(&frame.Frame{Type: frame.Pong, Session: s.ID}, true)
	}
}

func (h *Hub) fanOutBulkLocked(s *Session, from *Participant, f *frame.Frame) {
	if s.master != nil && s.master != from && s.master.bulk != nil {
		s.master.bulk.sendFrame(f, false)
	}
	for _, m := range s.members {
		if m == from || m == s.master || m.bulk == nil {
			continue
		}
		m.bulk.sendFrame(f, false)
	}
	for _, observer := range s.observers {
		if observer != from && observer.bulk != nil {
			observer.bulk.sendFrame(f, false)
		}
	}
}

// Sessions zählt die laufenden Sitzungen (Tests, Health).
func (h *Hub) Sessions() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}
