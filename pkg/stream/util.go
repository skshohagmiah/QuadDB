package stream

import (
	"math/rand"
	"time"
)

// jitterDuration applies +/- jitter fraction to a base duration.
func jitterDuration(base time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	if jitter > 1 {
		jitter = 1
	}
	// random in [-jitter, +jitter]
	frac := (rand.Float64()*2 - 1) * jitter
	delta := time.Duration(float64(base) * frac)
	d := base + delta
	if d < 0 {
		return 0
	}
	return d
}
