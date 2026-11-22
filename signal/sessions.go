package signal

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// SessionParticipant ist ein Teilnehmer in der Sitzungsauskunft.
type SessionParticipant struct {
	ID   uint16 `json:"id"`
	Name string `json:"name"`
	Mode uint8  `json:"mode"`
	Rank uint16 `json:"rank"`
}

// SessionInfo ist die Auskunft über eine Sitzung (SIGNAL.md, „Sitzungsauskunft“). Wer das Dokument lesen darf,
// bekommt, dass sie läuft: App, Zahl der Teilnehmer, ob gesperrt. Namen, Master, Zähler und Puffer bekommt nur,
// wer selbst an der Sitzung teilnimmt, und die Betriebsrolle. Inhalte stehen nie darin.
type SessionInfo struct {
	Session      uint32 `json:"session"`
	Doc          string `json:"doc"`
	App          string `json:"app"`
	AppVersion   string `json:"appVersion"`
	Locked       bool   `json:"locked"`
	Participants int    `json:"participants"`
	// Detail sagt, ob die folgenden Felder gefüllt sind.
	Detail    bool                 `json:"detail"`
	Seq       uint32               `json:"seq,omitempty"`
	Master    uint16               `json:"master,omitempty"`
	Members   []SessionParticipant `json:"members,omitempty"`
	BlobBytes int                  `json:"blobBytes,omitempty"`
	// Observers sind die verdeckten Beobachter; nur die Betriebsrolle bekommt sie.
	Observers []SessionParticipant `json:"observers,omitempty"`
}

// SessionsResponse ist die Antwort von GET /signal/sessions.
type SessionsResponse struct {
	Sessions []SessionInfo `json:"sessions"`
}

// readable sagt, ob der Nutzer das Dokument lesen darf.
func (h *Hub) readable(r *http.Request, user, doc string) bool {
	_, ok := h.rights(r, user, doc)
	return ok
}

func (h *Hub) isOperator(user string) bool {
	return h.opts.Operator != nil && h.opts.Operator(user)
}

// used ist, wie viele Bytes der Puffer hält.
func (s *blobStore) usedBytes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// observersLocked nennt die verdeckten Beobachter einer Sitzung.
func (s *Session) observersLocked() []SessionParticipant {
	observers := make([]SessionParticipant, 0, len(s.observers))
	for _, observer := range s.observers {
		observers = append(observers, SessionParticipant{ID: observer.ID, Name: observer.Name, Mode: observer.Mode, Rank: observer.Rank})
	}
	sort.Slice(observers, func(i, j int) bool { return observers[i].ID < observers[j].ID })
	return observers
}

// infoLocked beschreibt eine Sitzung für einen Fragenden; mit detail auch Namen, Master, Zähler und Puffer.
// Verdeckte Beobachter stehen nicht darin und zählen nicht mit.
func (s *Session) infoLocked(detail bool) SessionInfo {
	info := SessionInfo{Session: s.ID, Doc: s.Doc, App: s.App, AppVersion: s.AppVersion, Locked: s.lock != "", Participants: len(s.members), Detail: detail}
	if !detail {
		return info
	}
	info.Seq = s.Seq
	if s.master != nil {
		info.Master = s.master.ID
	}
	if s.blobs != nil {
		info.BlobBytes = s.blobs.usedBytes()
	}
	info.Members = make([]SessionParticipant, 0, len(s.members))
	for _, member := range s.members {
		info.Members = append(info.Members, SessionParticipant{ID: member.ID, Name: member.Name, Mode: member.Mode, Rank: member.Rank})
	}
	sort.Slice(info.Members, func(i, j int) bool { return info.Members[i].ID < info.Members[j].ID })
	return info
}

// takesPartLocked sagt, ob der Nutzer der Sitzung beigetreten ist (in welchem Modus auch immer).
func (s *Session) takesPartLocked(user string) bool {
	for _, member := range s.members {
		if member.User == user {
			return true
		}
	}
	return false
}

// handleSessions beantwortet GET /signal/sessions: die Sitzungen, die der Fragende sehen darf, mit ?doc= die zu
// einem Dokument. Eine Sitzung, die er nicht sehen darf, gibt es für ihn nicht (404 wie bei keiner Sitzung).
func (h *Hub) handleSessions(w http.ResponseWriter, r *http.Request) {
	user, err := h.authorize(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	wantedDoc := strings.TrimSpace(r.URL.Query().Get("doc"))
	operator := h.isOperator(user)

	// Die Rechteprüfung kann nach außen gehen (OpenCore): erst die Sitzungen unter der Sperre abschreiben, dann
	// ohne Sperre prüfen.
	type candidate struct {
		summary, detail SessionInfo
		takesPart       bool
		observers       []SessionParticipant
	}
	h.mu.Lock()
	candidates := make([]candidate, 0, len(h.sessions))
	for doc, session := range h.sessions {
		if wantedDoc != "" && doc != wantedDoc {
			continue
		}
		candidates = append(candidates, candidate{summary: session.infoLocked(false), detail: session.infoLocked(true), takesPart: session.takesPartLocked(user), observers: session.observersLocked()})
	}
	h.mu.Unlock()

	response := SessionsResponse{Sessions: []SessionInfo{}}
	for _, found := range candidates {
		switch {
		case operator:
			found.detail.Observers = found.observers
			response.Sessions = append(response.Sessions, found.detail)
		case found.takesPart:
			response.Sessions = append(response.Sessions, found.detail)
		case h.readable(r, user, found.summary.Doc):
			response.Sessions = append(response.Sessions, found.summary)
		}
	}
	if wantedDoc != "" && len(response.Sessions) == 0 {
		http.Error(w, "no session", http.StatusNotFound)
		return
	}
	sort.Slice(response.Sessions, func(i, j int) bool { return response.Sessions[i].Session < response.Sessions[j].Session })
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(response)
}
