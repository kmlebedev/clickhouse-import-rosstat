package ingest

import (
	"math"
	"sync"
	"time"
)

// DefaultRatePerMin — лимит запросов в минуту по умолчанию (INGEST_RATE_PER_MIN в cmd/ingest).
const DefaultRatePerMin = 60

// rateBurst — размер «ведра»: сколько запросов можно сделать подряд сверх средней частоты.
const rateBurst = 10

// tokenBucket — ограничитель частоты по алгоритму token bucket на stdlib.
// Ведро вмещает burst токенов и пополняется со скоростью ratePerMin/минуту; запрос берёт один токен.
// now внедряется для тестов; в проде — time.Now.
type tokenBucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	perSec   float64
	last     time.Time
	now      func() time.Time
}

func newTokenBucket(ratePerMin int, burst int, now func() time.Time) *tokenBucket {
	return &tokenBucket{
		capacity: float64(burst),
		tokens:   float64(burst),
		perSec:   float64(ratePerMin) / 60,
		last:     now(),
		now:      now,
	}
}

// allow снимает один токен; false — лимит исчерпан (ответ 429).
func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.now()
	if elapsed := current.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.perSec)
		b.last = current
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
