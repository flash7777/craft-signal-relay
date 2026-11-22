# Craft Signal: Relais (Go)

Das Relais von Craft Signal (Protokoll: `SIGNAL.md` im Projekt opencraft, nicht öffentlich) als eigenständiges Binary für Testbench und Offline-Betrieb. Im OpenCore-Kern läuft derselbe
Hub (`signal.Hub`) später als Modul hinter `/signal`; das Protokoll bleibt gleich.

```
go build ./cmd/relay && ./relay -dev -listen 127.0.0.1:8787
curl -X POST localhost:8787/signal/ticket -H 'Authorization: Bearer x' -H 'X-Signal-User: anna' \
     -d '{"doc":"<spaceid!fileid>","app":"wordcraft","appVersion":"0.18.1-kosmos.5","mode":1}'
```

- Das Binary `cmd/relay` beantwortet CORS-Vorabfragen und erlaubt jede Herkunft (`devCORS`): die WASM-Testbench lädt
  die App von einem anderen lokalen Port. Der Hub selbst (`signal.Hub`) setzt keine CORS-Header; im OpenCore-Kern ist
  `/signal` gleicher Herkunft.
- `POST /signal/ticket` → `{ticket, session?, app?, appVersion?, maxMode, reason?}`. Ohne `-dev` fehlt die Anmeldung
  gegen OpenCore (Bearer/App-Token → Nutzer, Rechte → `maxMode`); `-dev` nimmt jede Authorization, `X-Signal-User`
  nennt den Nutzer.
- Verdecktes Ticket (Recorder): `POST /signal/ticket` mit `"hidden": true` → nur lesen, für die anderen unsichtbar
  (kein `ROSTER`, zählt nicht, kein Rang, eröffnet und hält keine Sitzung). Braucht `Options.Hidden` (Rolle; nicht
  gesetzt = abgeschaltet), sonst 403. Im Dev-Binary: `-hidden a,b`. Die Betriebsrolle sieht Beobachter in der
  Sitzungsauskunft unter `observers`. Der Beobachter bekommt neben dem, was ein Leser bekommt, auch `OP`
  und `NAK` (Debugging).
- `GET /signal/sessions[?doc=<Dokument-ID>]` → `{sessions: [{session, doc, app, appVersion, locked, participants,
  detail, seq?, master?, members?, blobBytes?}]}`. Wer das Dokument lesen darf, sieht, dass eine Sitzung läuft;
  Namen, Master, Zähler und Puffer (`detail: true`) nur, wer beigetreten ist, und die Betriebsrolle. Mit `?doc=`
  und ohne sichtbare Sitzung 404. Leserecht und Betriebsrolle hängt OpenCore ein (`Options.MayRead`,
  `Options.Operator`); im Dev-Binary darf jeder Angemeldete lesen, die Betriebsrolle setzt `-operators a,b`.
- `GET /signal` (Steuerkanal) und `GET /signal/bulk` (Bulk-Kanal): WebSocket, erster Rahmen `HELLO` mit dem Ticket
  (beide Kanäle dasselbe Ticket, Bulk mit `b: true`), dann `WELCOME`.
- Pakete: `frame` (24-Byte-Kopf), `signal` (Hub, Wire-Nutzlasten in CBOR, Sitzungspuffer für Blobs).
- Tests: `go test ./...` (zwei bis drei Testclients je Fall, kein Netz außer loopback).

Abhängigkeiten: `github.com/coder/websocket` (ISC), `github.com/fxamacker/cbor/v2` (MIT).
