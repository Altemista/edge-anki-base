package anki

import (
	"time"
)

func Start_execution_time(s string) (string, time.Time) {
	plog.Println("========== Start ==========	", s)
	return s, time.Now()
}

func Track_execution_time(s string, startTime time.Time) {
	plog.Println("========== End ==========", s, "took", time.Since(startTime).Seconds()*1000, "ms")
}