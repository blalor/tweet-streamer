package main

import (
    "fmt"
    "os"
    "syscall"

    log "github.com/Sirupsen/logrus"
    flags "github.com/jessevdk/go-flags"
    ti "github.com/blalor/elastic-tweeter/tweet_indexer"
    "github.com/ChimeraCoder/anaconda"
    "github.com/belogik/goes"
    "net/url"
    "strconv"
)

var version string = "undef"

type Options struct {
    Debug   bool   `env:"DEBUG"    long:"debug"    description:"enable debug"`
    LogFile string `env:"LOG_FILE" long:"log-file" description:"path to JSON log file"`

    ConsumerKey    string `               long:"consumer-key"    required:"true"`
    ConsumerSecret string `               long:"consumer-secret" required:"true"`
    AccessToken    string `               long:"access-token"    required:"true"`
    AccessSecret   string `               long:"access-secret"   required:"true"`
    Since          int64  `               long:"since"    description:"backfill tweets since this id"`

    ESHost string `long:"es-host" required:"true"`
    ESPort string `long:"es-port" required:"true"` // *string*?!
}

func main() {
    var opts Options

    _, err := flags.Parse(&opts)
    if err != nil {
        os.Exit(1)
    }

    if opts.Debug {
        log.SetLevel(log.DebugLevel)
    }

    if opts.LogFile != "" {
        logFp, err := os.OpenFile(opts.LogFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
        checkError(fmt.Sprintf("error opening %s", opts.LogFile), err)

        defer logFp.Close()

        // ensure panic output goes to log file
        syscall.Dup2(int(logFp.Fd()), 1)
        syscall.Dup2(int(logFp.Fd()), 2)

        // log as JSON
        log.SetFormatter(&log.JSONFormatter{})

        // send output to file
        log.SetOutput(logFp)
    }

    log.Debug("hi there! (tickertape tickertape)")
    log.Infof("version: %s", version)

    anaconda.SetConsumerKey(opts.ConsumerKey)
    anaconda.SetConsumerSecret(opts.ConsumerSecret)
    twitter := anaconda.NewTwitterApi(opts.AccessToken, opts.AccessSecret)

    twitter.SetLogger(loggerAdapter{log.WithField("component", "anaconda")})

    indexer := ti.New(twitter, goes.NewConnection(opts.ESHost, opts.ESPort))
    go indexer.Run()

    if opts.Since > 0 {
        v := url.Values{}
        v.Set("count", strconv.FormatInt(200, 10)) // max allowed for a single request
        v.Set("since_id", strconv.FormatInt(opts.Since, 10))
        tweets, err := twitter.GetHomeTimeline(v)

        checkError("unable to get home timeline", err)

        for _, t := range tweets {
            indexer.Index(t)
        }
    }

    // start consuming the stream
    stream := twitter.UserStream(nil)

    for streamObj := range stream.C {
        switch t := streamObj.(type) {
        default:
            log.Warnf("unhandled type %T", t)
        case anaconda.Tweet:
            indexer.Index(t)
        case anaconda.EventTweet:
            // (un)favorite, or others
            switch t.Event.Event {
            default:
                log.Warnf("unhandled event '%s'", t.Event)
            case "favorite":
                indexer.UpdateFavorite(t.TargetObject, true)
            case "unfavorite":
                indexer.UpdateFavorite(t.TargetObject, false)
            }
        }
    }
}
