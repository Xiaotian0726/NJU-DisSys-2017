package raft

import (
	"io"
	"log"
	"os"
)

//const Debug = 0
const Debug = 1

var (
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
)

func PathExist(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

func init() {
	if PathExist("./log") == false {
		os.Mkdir("log", os.ModePerm)
	}

	infoFile, err := os.OpenFile("./log/info.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Open infoFile failed.\n", err)
	}
	warnFile, err := os.OpenFile("./log/warn.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Open warnFile failed.\n", err)
	}
	errorFile, err := os.OpenFile("./log/err.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Open errorFile failed.\n", err)
	}

	Info = log.New(io.MultiWriter(os.Stderr, infoFile), "Info: ", log.Ldate|log.Ltime|log.Lshortfile)
	Warn = log.New(io.MultiWriter(os.Stderr, warnFile), "Warn: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(os.Stderr, errorFile), "Error: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func DPrintf(printLog bool, level string, format string, a ...interface{}) (n int, err error) {
	//if Debug > 0 {
	//	log.Printf(format, a...)
	//}

	if Debug == 0 || !printLog {
		return
	}

	switch level {
	case "info":
		Info.Printf(format, a...)
	case "warn":
		Warn.Printf(format, a...)
	case "error":
		Error.Printf(format, a...)
	}

	return
}

func Min(a, b int) int {
	if a < b{
		return a
	}
	return b
}
func Max(a, b int) int {
	if a < b{
		return b
	}
	return a
}