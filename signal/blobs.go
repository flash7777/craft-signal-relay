package signal

import "sync"

// DefaultBlobBudget ist der Sitzungspuffer für Blobs (SIGNAL.md Bulk-Kanal: Vorschlag 256 MB je Sitzung).
const DefaultBlobBudget = 256 * 1024 * 1024

// blob sind die bisher gesehenen Teile eines Blobs, nach Offset.
type blob struct {
	parts map[uint64][]byte
	size  int
	done  bool
}

// blobStore hält die Blobs einer Sitzung im Speicher, mit Obergrenze: der älteste fliegt zuerst raus.
type blobStore struct {
	mu     sync.Mutex
	budget int
	used   int
	blobs  map[[HashSize]byte]*blob
	order  [][HashSize]byte
}

func newBlobStore(budget int) *blobStore {
	return &blobStore{budget: budget, blobs: map[[HashSize]byte]*blob{}}
}

// put merkt sich einen Teil; ein Teil über dem ganzen Budget wird nicht gehalten (nur weitergeleitet).
func (s *blobStore) put(part *BlobPart) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(part.Data) > s.budget {
		return
	}
	b, ok := s.blobs[part.Hash]
	if !ok {
		b = &blob{parts: map[uint64][]byte{}}
		s.blobs[part.Hash] = b
		s.order = append(s.order, part.Hash)
	}
	if old, seen := b.parts[part.Offset]; seen {
		s.used -= len(old)
		b.size -= len(old)
	}
	b.parts[part.Offset] = part.Data
	b.size += len(part.Data)
	s.used += len(part.Data)
	for s.used > s.budget && len(s.order) > 0 {
		oldest := s.order[0]
		s.order = s.order[1:]
		if victim, ok := s.blobs[oldest]; ok {
			s.used -= victim.size
			delete(s.blobs, oldest)
		}
	}
}

// finish merkt sich, dass der Absender den Blob für vollständig erklärt hat.
func (s *blobStore) finish(hash [HashSize]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.blobs[hash]; ok {
		b.done = true
	}
}

// parts liefert die gehaltenen Teile eines Blobs, nach Offset, und ob er vollständig gemeldet ist.
func (s *blobStore) parts(hash [HashSize]byte) ([]*BlobPart, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blobs[hash]
	if !ok {
		return nil, false
	}
	out := make([]*BlobPart, 0, len(b.parts))
	for offset, data := range b.parts {
		out = append(out, &BlobPart{Hash: hash, Offset: offset, Data: data})
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Offset < out[j-1].Offset; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, b.done
}
