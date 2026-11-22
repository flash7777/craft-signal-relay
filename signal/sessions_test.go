package signal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// askSessions fragt die Sitzungsauskunft als Nutzer, mit ?doc= wenn doc nicht leer ist.
func askSessions(t *testing.T, server *httptest.Server, user, doc string) (int, SessionsResponse) {
	t.Helper()
	url := server.URL + "/signal/sessions"
	if doc != "" {
		url += "?doc=" + doc
	}
	req, _ := http.NewRequest("GET", url, nil)
	if user != "" {
		req.Header.Set("Authorization", "Bearer token-"+user)
		req.Header.Set("X-Signal-User", user)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var answer SessionsResponse
	if resp.StatusCode == 200 {
		if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, answer
}

// serverWith ist ein Relais, in dem nur readers je Dokument lesen dürfen und operator die Betriebsrolle hat.
func serverWith(t *testing.T, readers map[string][]string, operator string) *httptest.Server {
	t.Helper()
	hub := New(Options{
		Dev:        true,
		BlobBudget: 1 << 20,
		Rights: func(_ *http.Request, user, doc string) (uint8, bool) {
			for _, reader := range readers[doc] {
				if reader == user {
					return ModeEdit, true
				}
			}
			return ModeReadOnly, false
		},
		Operator: func(user string) bool { return user == operator },
	})
	mux := http.NewServeMux()
	hub.Routes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestParticipantsSeeNamesAndMasterReadersOnlyThatASessionRuns(t *testing.T) {
	server := serverWith(t, map[string][]string{"doc-a": {"anna", "ben", "clara"}}, "root")
	anna := join(t, server, "anna", "doc-a", "wordcraft", "1", ModeEdit)
	ben := join(t, server, "ben", "doc-a", "wordcraft", "1", ModeReadOnly)

	// Wer beigetreten ist (auch nur lesend), bekommt alles.
	for _, user := range []string{"anna", "ben"} {
		status, answer := askSessions(t, server, user, "doc-a")
		if status != 200 || len(answer.Sessions) != 1 {
			t.Fatalf("%s: status %d, %d sessions", user, status, len(answer.Sessions))
		}
		info := answer.Sessions[0]
		if !info.Detail || info.Master != anna.id || len(info.Members) != 2 || info.Members[0].Name != "anna" || info.Members[1].Name != "ben" {
			t.Fatalf("%s: %+v", user, info)
		}
		if info.Members[1].ID != ben.id || info.Members[1].Mode != ModeReadOnly || info.App != "wordcraft" || !info.Locked {
			t.Fatalf("%s: %+v", user, info)
		}
	}

	// Wer das Dokument nur lesen darf: dass eine Sitzung läuft, ohne Namen, Master und Zähler.
	status, answer := askSessions(t, server, "clara", "doc-a")
	if status != 200 || len(answer.Sessions) != 1 {
		t.Fatalf("clara: status %d, %d sessions", status, len(answer.Sessions))
	}
	info := answer.Sessions[0]
	if info.Detail || info.Members != nil || info.Master != 0 || info.Seq != 0 || info.Participants != 2 || info.App != "wordcraft" || !info.Locked {
		t.Fatalf("clara: %+v", info)
	}
	raw, _ := json.Marshal(info)
	for _, name := range []string{"anna", "ben"} {
		if strings.Contains(string(raw), name) {
			t.Fatalf("clara sees a name: %s", raw)
		}
	}
}

func TestASessionOneMayNotSeeDoesNotExistForThem(t *testing.T) {
	server := serverWith(t, map[string][]string{"doc-a": {"anna"}, "doc-b": {"ben", "dora"}}, "root")
	join(t, server, "anna", "doc-a", "wordcraft", "1", ModeEdit)
	join(t, server, "ben", "doc-b", "wordcraft", "1", ModeEdit)

	// Ohne Leserecht: wie keine Sitzung.
	if status, _ := askSessions(t, server, "dora", "doc-a"); status != 404 {
		t.Fatalf("dora on doc-a: %d", status)
	}
	if status, _ := askSessions(t, server, "dora", "doc-none"); status != 404 {
		t.Fatalf("no session: %d", status)
	}
	// Die Liste: nur was man sehen darf, je nach Rolle ganz oder knapp.
	_, doras := askSessions(t, server, "dora", "")
	if len(doras.Sessions) != 1 || doras.Sessions[0].Doc != "doc-b" || doras.Sessions[0].Detail {
		t.Fatalf("dora: %+v", doras.Sessions)
	}
	_, annas := askSessions(t, server, "anna", "")
	if len(annas.Sessions) != 1 || annas.Sessions[0].Doc != "doc-a" || !annas.Sessions[0].Detail {
		t.Fatalf("anna: %+v", annas.Sessions)
	}
	// Die Betriebsrolle sieht alle Sitzungen ganz, auch ohne Leserecht am Dokument.
	_, roots := askSessions(t, server, "root", "")
	if len(roots.Sessions) != 2 || !roots.Sessions[0].Detail || !roots.Sessions[1].Detail || len(roots.Sessions[1].Members) != 1 {
		t.Fatalf("root: %+v", roots.Sessions)
	}
	// Ohne Anmeldung gibt es nichts.
	if status, _ := askSessions(t, server, "", ""); status != 401 {
		t.Fatalf("anonymous: %d", status)
	}
}

func TestWithoutAReadCheckOnlyDevAnswersReaders(t *testing.T) {
	// Dev ohne eingehängte Prüfung: jeder Angemeldete sieht, dass eine Sitzung läuft.
	_, server := newServer(t)
	join(t, server, "anna", "doc-a", "wordcraft", "1", ModeEdit)
	status, answer := askSessions(t, server, "zoe", "doc-a")
	if status != 200 || answer.Sessions[0].Detail {
		t.Fatalf("dev: %d %+v", status, answer.Sessions)
	}
	// Ohne Dev und ohne Prüfung: niemand außer Teilnehmern und Betriebsrolle.
	hub := New(Options{Authorize: func(r *http.Request) (string, error) { return r.Header.Get("X-Signal-User"), nil }})
	if _, ok := hub.rights(nil, "zoe", "doc-a"); ok {
		t.Fatal("without a rights check and without dev nobody may read")
	}
}

func TestTheTicketIsLimitedByTheRightsToTheDocument(t *testing.T) {
	hub := New(Options{
		Dev: true,
		Rights: func(_ *http.Request, user, _ string) (uint8, bool) {
			switch user {
			case "anna":
				return ModeEdit, true
			case "ben":
				return ModeReadOnly, true
			}
			return ModeReadOnly, false
		},
		DisplayName: func(r *http.Request) string { return "Name of " + r.Header.Get("X-Signal-User") },
	})
	mux := http.NewServeMux()
	hub.Routes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ask := func(user string) (int, TicketResponse) {
		body, _ := json.Marshal(TicketRequest{Doc: "doc-r", App: "wordcraft", AppVersion: "1", Mode: ModeEdit, Name: "claimed"})
		req, _ := http.NewRequest("POST", server.URL+"/signal/ticket", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer token-"+user)
		req.Header.Set("X-Signal-User", user)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var ticket TicketResponse
		if resp.StatusCode == 200 {
			_ = json.NewDecoder(resp.Body).Decode(&ticket)
		}
		return resp.StatusCode, ticket
	}
	if status, ticket := ask("anna"); status != 200 || ticket.MaxMode != ModeEdit {
		t.Fatalf("may edit: %d %+v", status, ticket)
	}
	if status, ticket := ask("ben"); status != 200 || ticket.MaxMode != ModeReadOnly {
		t.Fatalf("may only read: %d %+v", status, ticket)
	}
	if status, _ := ask("zoe"); status != http.StatusForbidden {
		t.Fatalf("no access: %d", status)
	}
	// Der Name kommt von der Anmeldung, nicht aus der Anfrage.
	a := join(t, server, "anna", "doc-r", "wordcraft", "1", ModeEdit)
	_, own := askSessions(t, server, "anna", "doc-r")
	if own.Sessions[0].Members[0].ID != a.id || own.Sessions[0].Members[0].Name != "Name of anna" {
		t.Fatalf("name: %+v", own.Sessions[0].Members)
	}
}
