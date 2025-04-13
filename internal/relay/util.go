package relay

import "time"

func makeTickerChan(interval time.Duration) (<-chan time.Time, *time.Ticker) {
	if interval <= 0 {
		return make(chan time.Time), nil
	}
	t := time.NewTicker(interval)
	return t.C, t
}

func nextBackoff(prev, max time.Duration) time.Duration {
	if prev <= 0 {
		return 500 * time.Millisecond
	}
	return min(prev*2, max)
}
