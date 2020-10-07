package raft

import "time"

type Timer chan int

func MakeTimer(milliseconds time.Duration) Timer {
	t := make(Timer, 1)

	go func (timer Timer) {
		time.Sleep(milliseconds * time.Millisecond)
		timer <- 1
		close(timer)
	}(t)

	return t
}