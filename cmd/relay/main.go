// relay ist das Craft-Signal-Relais als eigenständiges Binary (SIGNAL.md): für die Testbench und den
// Offline-Betrieb. Im OpenCore-Kern läuft derselbe Hub als Modul hinter /signal.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"forge.mux.nu/xworx/craft-signal-relay/signal"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8787", "Adresse, auf der das Relais hört")
	dev := flag.Bool("dev", false, "Dev-Modus: jede nicht leere Authorization gilt, Sperren werden simuliert")
	blobBudget := flag.Int("blob-budget", signal.DefaultBlobBudget, "Sitzungspuffer für Blobs in Byte")
	operators := flag.String("operators", "", "Dev: Nutzer mit Betriebsrolle für die Sitzungsauskunft, durch Komma getrennt")
	hidden := flag.String("hidden", "", "Dev: Nutzer, die verdeckt beitreten dürfen (Recorder), durch Komma getrennt; leer = abgeschaltet")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if !*dev {
		log.Error("ohne -dev fehlt die Anmeldung gegen OpenCore; bis sie da ist, nur -dev")
		os.Exit(2)
	}
	operatorNames, hiddenNames := nameSet(*operators), nameSet(*hidden)
	options := signal.Options{Dev: *dev, BlobBudget: *blobBudget, Log: log, Operator: func(user string) bool { return operatorNames[user] }}
	if len(hiddenNames) > 0 {
		options.Hidden = func(user string) bool { return hiddenNames[user] }
	}
	hub := signal.New(options)
	mux := http.NewServeMux()
	hub.Routes(mux)
	mux.HandleFunc("GET /signal/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"sessions":` + itoa(hub.Sessions()) + `}` + "\n"))
	})
	// Dev: die Web-App läuft lokal auf einer anderen Herkunft (Trunk-Ausgabe auf eigenem Port); die Ticketanfrage
	// per fetch braucht dann CORS. Im OpenCore-Kern ist /signal gleicher Herkunft, dort gibt es das nicht.
	server := &http.Server{Addr: *listen, Handler: devCORS(mux), ReadHeaderTimeout: 10 * time.Second}
	log.Info("craft signal relay", "listen", *listen, "dev", *dev)
	if err := server.ListenAndServe(); err != nil {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}

// devCORS beantwortet die Vorabfrage des Browsers und erlaubt jede Herkunft; nur im Dev-Binary.
func devCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Signal-User")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// nameSet liest eine durch Komma getrennte Liste von Nutzern.
func nameSet(list string) map[string]bool {
	names := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names[name] = true
		}
	}
	return names
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
