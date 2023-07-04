package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		_, file, line, _ := runtime.Caller(1)
		file = filepath.Base(file) // get just the filename
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("Topic: %s Time: %06d File: %v Line: %v ", string(topic), time, file, line)
		format = prefix + format
		log.Printf(format, a...)
		// time := time.Since(debugStart).Microseconds()
		// time /= 100
		// prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		// format = prefix + format
		// log.Printf(format, a...)
	}
}
