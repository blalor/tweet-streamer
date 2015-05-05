package tweet_indexer

import (
    "time"

    log "github.com/Sirupsen/logrus"

    "github.com/belogik/goes"
    "github.com/ChimeraCoder/anaconda"
)

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

type TweetIndexer struct {
    es        *goes.Connection
    twitter   *anaconda.TwitterApi
    tweetChan chan anaconda.Tweet
}

func New(twitter *anaconda.TwitterApi, elasticsearch *goes.Connection) *TweetIndexer {
    return &TweetIndexer{
        es:        elasticsearch,
        twitter:   twitter,
        tweetChan: make(chan anaconda.Tweet),
    }
}

func (self *TweetIndexer) Index(tweet anaconda.Tweet) {
    self.tweetChan <- tweet
}

func (self *TweetIndexer) UpdateFavorite(tweet *anaconda.Tweet, favorited bool) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "filtered": map[string]interface{}{
                "query": map[string]interface{}{
                    "match_all": map[string]interface{}{},
                },
                "filter": map[string]interface{}{
                    "or": []map[string]interface{}{
                        map[string]interface{}{
                            "ids": map[string]interface{}{
                                "values": []string{
                                    tweet.IdStr,
                                },
                            },
                        },
                        map[string]interface{}{
                            "term": map[string]interface{}{
                                "retweeted.OriginalId": tweet.IdStr,
                            },
                        },
                    },
                },
            },
        },
    }

    results, err := self.es.Search(query, []string{"twitter-*"}, []string{"tweet"}, nil)
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

        self.tweetChan <- *tweet
    } else if results.Hits.Total != 1 {
        log.Errorf("found %d tweets in ES for id %s, expected one", results.Hits.Total, tweet.IdStr)
    } else {
        // found one document in ES, as expected; just update it
        hit := results.Hits.Hits[0]

        log.Debugf("updating favorited of %s to %t", hit.Id, favorited)

        _, err = self.es.Update(
            goes.Document{
                Index: hit.Index,
                Type:  hit.Type,
                Id:    hit.Id,
            },
            map[string]interface{}{
                "doc": map[string]bool{
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

func (self *TweetIndexer) Run() {
    for t := range self.tweetChan {
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
            target, err := self.twitter.GetTweet(t.InReplyToStatusID, nil)
            if err != nil {
                log.Warnf("unable to the tweet %d is in reply to: %v", t.InReplyToStatusID, err)
            } else {
                // need to dispatch in a goroutine or we'll block because we
                // can't write to the same channel we're reading from
                go func() {
                    self.Index(target)
                    log.Debugf("dispatched replied-to tweet %d", target.Id)
                }()
            }
        }

        _, err = self.es.Index(goes.Document{
            Index:  "twitter-" + esTweet.CreatedTime.Format("2006.01.02"),
            Type:   "tweet",
            Id:     esTweet.Id,
            Fields: esTweet,
        }, nil)

        if err != nil {
            log.Fatalf("unable to index tweet: %+v", err)
        }
    }
}
