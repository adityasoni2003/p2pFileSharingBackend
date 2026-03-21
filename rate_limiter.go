package main

import (
	"sync"
	"time"
)

type RateLimiter struct {
	visitors map[string]*visitor
	mu       sync.Mutex
	rate     int
	window   int64
}

type visitor struct {
	count     int
	timestamp int64
}

func NewRateLimiter(rate int, window int64) *RateLimiter {
	return &RateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		window:   window,
	}
}

func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now().Unix()

	v, exists := rl.visitors[ip]

	if !exists || now-v.timestamp > rl.window {
		rl.visitors[ip] = &visitor{
			count:     1,
			timestamp: now,
		}
		return true
	}

	if v.count >= rl.rate {
		return false
	}

	v.count++
	return true
}
