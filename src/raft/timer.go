package raft

import "time"
import "math/rand"

type Timer chan int

func RandomTimeoutDuration(low, high int) time.Duration {
	return time.Duration(low + rand.Intn(high - low))
}

func StartTimer(milliseconds time.Duration) Timer {
	t := make(Timer, 1)

	go func (timer Timer) {
		time.Sleep(milliseconds * time.Millisecond)
		timer <- 1
		close(timer)
	}(t)

	return t
}