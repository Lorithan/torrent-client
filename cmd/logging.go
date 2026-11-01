package main

import (
	"log"
	"os"
)

func redirectLogs(path string) (func() error, error) {
	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	currentLogOut := log.Writer()
	log.SetOutput(logFile)

	return func() error {
		log.SetOutput(currentLogOut)
		return logFile.Close()
	}, nil
}
