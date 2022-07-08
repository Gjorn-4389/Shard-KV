package kvraft

import (
	"fmt"
	"log"
	"time"
)

// false true
const debug = false

var debugStart time.Time

func init() {
	debugStart = time.Now()
	// disable all datetime logging
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d ", time)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
