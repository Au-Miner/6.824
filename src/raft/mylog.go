package raft

import "os"
import (
	"github.com/sirupsen/logrus"
)

var Logger = logrus.New()

func init() {
	Logger.SetOutput(os.Stdout)
	Logger.SetLevel(logrus.InfoLevel)
}
