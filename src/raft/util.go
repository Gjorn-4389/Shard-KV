package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const (
	RejectAll   int = 100
	PrintAlways int = 99
	Lab2A       int = 10
	Lab2B       int = 20
	Lab2C       int = 30
	Lab2D       int = 40
)

const DebugLevel = RejectAll

var debugStart time.Time

func init() {
	debugStart = time.Now()
	// disable all datetime logging
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if level >= DebugLevel {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d ", time)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
