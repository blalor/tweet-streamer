package main

import (
	"fmt"
	"os"
	"syscall"

	log "github.com/Sirupsen/logrus"
	flags "github.com/jessevdk/go-flags"

	"github.com/ChimeraCoder/anaconda"
	"github.com/belogik/goes"
	"net/url"
	"strconv"
	"time"
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

// how it'll be stored in ES
type User struct {
	Id         int64
	Name       *string `json:",omitempty"`
	ScreenName string
}

func UserFromTwitter(user anaconda.User) User {
	return User{
		Id:         user.Id,
		Name:       &user.Name,
		ScreenName: user.ScreenName,
	}
}

type RetweetMeta struct {
	By *User
	// time a retweeted tweet was originally created
	OriginalCreatedTime time.Time
	OriginalId          string
}

type InReplyTo struct {
	User User
	Id   int64
}

type Media struct {
	Id   int64
	Type string
	Url  string
}

// if a retweet, contents of retweeted_status, else the main tweet (except for RetweetedBy)
type ElasticSearchTweet struct {
	Id string `json:"id"`
	// time tweet appeared in our timeline
	CreatedTime time.Time `json:"@timestamp"`
	User        User      `json:"user"`

	// with URLs expanded; so will not honor 140 chars
	Text string

	Coordinates *anaconda.Coordinates `json:"coordinates,omitempty"`

	// from Entities
	HashTags     []string `json:"hashtags,omitempty"`
	UserMentions []User   `json:"user_mentions,omitempty"`
	Urls         []string `json:"urls,omitempty"`
	Media        []Media  `json:"media,omitempty"`

	InReplyTo *InReplyTo `json:"in_reply_to,omitempty"`

	Retweeted *RetweetMeta `json:"retweeted,omitempty"`
	Favorited bool         `json:"favorited"`
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

	elasticsearch := goes.NewConnection(opts.ESHost, opts.ESPort)

	tweetChan := make(chan anaconda.Tweet)

	go func() {
		for t := range tweetChan {
			esTweet := ElasticSearchTweet{}

			esTweet.Id = t.IdStr
			esTweet.Favorited = t.Favorited
			createdTime, err := t.CreatedAtTime()
			if err != nil {
				log.Errorf("unable to parse time '%s': %s", t.CreatedAt, err)
				createdTime = time.Now()
			}

			esTweet.CreatedTime = createdTime

			if t.RetweetedStatus != nil {
				u := UserFromTwitter(t.User)

				origCreatedTime, err := t.RetweetedStatus.CreatedAtTime()
				if err != nil {
					log.Errorf("unable to parse time '%s': %s", t.CreatedAt, err)
					origCreatedTime = time.Now()
				}

				esTweet.Retweeted = &RetweetMeta{
					By:                  &u,
					OriginalCreatedTime: origCreatedTime,
					OriginalId:          t.RetweetedStatus.IdStr,
				}

				t = *t.RetweetedStatus
			}

			esTweet.User = UserFromTwitter(t.User)
			esTweet.Coordinates = t.Coordinates

			if len(t.Entities.Hashtags) > 0 {
				esTweet.HashTags = make([]string, len(t.Entities.Hashtags))

				for i, ht := range t.Entities.Hashtags {
					esTweet.HashTags[i] = ht.Text
				}
			}

			if len(t.Entities.User_mentions) > 0 {
				esTweet.UserMentions = make([]User, len(t.Entities.User_mentions))

				for i, um := range t.Entities.User_mentions {
					// need to assign to a local variable, or the names are
					// wrong for multiple mentions.
					name := um.Name
					esTweet.UserMentions[i] = User{
						Id:         um.Id,
						Name:       &name,
						ScreenName: um.Screen_name,
					}
				}
			}

			if len(t.Entities.Urls) > 0 {
				esTweet.Urls = make([]string, len(t.Entities.Urls))

				for i, u := range t.Entities.Urls {
					esTweet.Urls[i] = u.Expanded_url
				}
			}

			if len(t.Entities.Media) > 0 {
				esTweet.Media = make([]Media, len(t.Entities.Media))

				for i, m := range t.Entities.Media {
					esTweet.Media[i] = Media{
						Id:   m.Id,
						Type: m.Type,
						Url:  m.Media_url,
					}
				}
			}

			esTweet.Text = t.Text // @todo expand

			if t.InReplyToStatusID != 0 {
				esTweet.InReplyTo = &InReplyTo{
					User: User{
						Id:         t.InReplyToUserID,
						ScreenName: t.InReplyToScreenName,
					},
					Id: t.InReplyToStatusID,
				}

				log.Debugf("retrieving target tweet %d is in reply to", t.InReplyToStatusID)
				target, err := twitter.GetTweet(t.InReplyToStatusID, nil)
				if err != nil {
					log.Warnf("unable to the tweet %d is in reply to: %v", t.InReplyToStatusID, err)
				} else {
					// need to dispatch in a goroutine or we'll block because we
					// can't write to the same channel we're reading from
					go func() {
						tweetChan <- target
						log.Debugf("dispatched replied-to tweet %d", target.Id)
					}()
				}
			}

			_, err = elasticsearch.Index(goes.Document{
				Index:  "twitter-" + esTweet.CreatedTime.Format("2006.01.02"),
				Type:   "tweet",
				Id:     esTweet.Id,
				Fields: esTweet,
			}, nil)

			checkError("unable to index tweet", err)
		}
	}()

	updateFavorite := func(tweet *anaconda.Tweet, favorited bool) {
		query := map[string]interface{} {
		    "query": map[string]interface{} {
		        "filtered": map[string]interface{} {
		            "query": map[string]interface{} {
		                "match_all": map[string]interface{} {},
		            },
		            "filter": map[string]interface{} {
		                "or": []map[string]interface{} {
		                    map[string]interface{} {
		                        "ids": map[string]interface{} {
		                            "values": []string{
		                                tweet.IdStr,
		                            },
		                        },
		                    },
		                    map[string]interface{} {
		                        "term": map[string]interface{} {
		                            "retweeted.OriginalId": tweet.IdStr,
		                        },
		                    },
		                },
		            },
		        },
		    },
		}
		
		results, err := elasticsearch.Search(query, []string{"twitter-*"}, []string{"tweet"}, nil)
		if err != nil {
			log.Errorf("error searching for tweet in ES with id %s: %s", tweet.IdStr, err)
			return
		}
		
		if results.Hits.Total == 0 {
			// hm, we haven't seen this tweet before
			log.Debugf("no tweet found in ES with id %s", tweet.IdStr)
			
			// the TargetObject's "favorite" property doesn't reflect the change
			// here.
			tweet.Favorited = favorited
			
			tweetChan <- *tweet
		} else if results.Hits.Total != 1 {
			log.Errorf("found %d tweets in ES for id %s, expected one", results.Hits.Total, tweet.IdStr)
		} else {
			// found one document in ES, as expected; just update it
			hit := results.Hits.Hits[0]

			log.Debugf("updating favorited of %s to %t", hit.Id, favorited)
			
			_, err = elasticsearch.Update(
				goes.Document{
					Index:  hit.Index,
					Type:   hit.Type,
					Id:     hit.Id,
				},
				map[string]interface{} {
					"doc": map[string]bool {
						"favorited": favorited,
					},
				},
				nil,
			)
			
			if err != nil {
				log.Errorf("unable to update favorited of %s: %s", hit.Id, err)
			}
		}
	}
	
	if opts.Since > 0 {
		v := url.Values{}
		v.Set("count", strconv.FormatInt(200, 10)) // max allowed for a single request
		v.Set("since_id", strconv.FormatInt(opts.Since, 10))
		tweets, err := twitter.GetHomeTimeline(v)

		checkError("unable to get home timeline", err)

		for _, t := range tweets {
			tweetChan <- t
		}
	}

	// start consuming the stream
	stream := twitter.UserStream(nil)

	for streamObj := range stream.C {
		switch t := streamObj.(type) {
		default:
			log.Warnf("unhandled type %T", t)
		case anaconda.Tweet:
			tweetChan <- streamObj.(anaconda.Tweet)
		case anaconda.EventTweet:
			// (un)favorite, or others
			switch t.Event.Event {
			default:
				log.Warnf("unhandled event '%s'", t.Event)
			case "favorite":
				updateFavorite(t.TargetObject, true)
			case "unfavorite":
				updateFavorite(t.TargetObject, false)
			}
		}
	}
}
