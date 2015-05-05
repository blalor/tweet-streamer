package main

import log "github.com/Sirupsen/logrus"

type loggerAdapter struct {
    *log.Entry
}

func (self loggerAdapter) Critical(args ...interface{}) {
    self.WithField("critical", true).Error(args)
}

func (self loggerAdapter) Criticalf(format string, args ...interface{}) {
    self.WithField("critical", true).Errorf(format, args)
}

func (self loggerAdapter) Notice(args ...interface{}) {
    self.Info(args)
}

func (self loggerAdapter) Noticef(format string, args ...interface{}) {
    self.Infof(format, args)
}
