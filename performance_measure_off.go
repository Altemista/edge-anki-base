// +build !performance

package anki

import (
	"time"
)

func Start_execution_time(s string) (string, time.Time) {
	return s, time.Now()
}

func Track_execution_time(s string, startTime time.Time) {

}