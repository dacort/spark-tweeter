package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dghubble/go-twitter/twitter"
)

// tweetMonitor creates a new Tweet thread and appends subsequent messages to the thread.
// The creating caller is responsible for `close`ing the channel when done.
func tweetMonitor(ctx context.Context, wg *sync.WaitGroup) chan string {
	client := getTwitterClient()
	tweetText := make(chan string)

	// We keep track of tweetIDs here in order to create a thread
	var tweetID int64

	go func() {
		defer wg.Done()
		for statusText := range tweetText {
			params := twitter.StatusUpdateParams{}

			log.Println("Sending a tweet!", statusText)
			if tweetID > 0 {
				params.InReplyToStatusID = tweetID
			}
			tweet, _, err := client.Statuses.Update(statusText, &params)
			if err != nil {
				log.Println(err)
			} else {
				tweetID = tweet.ID
			}
		}
	}()

	return tweetText
}

// sparkAPIMonitor keeps an eye on the Spark endpoint and sends possibly pithy
// messages to the provided tweeterChannel.
func sparkAPIMonitor(ctx context.Context, wg *sync.WaitGroup, tweeterChannel chan string) {
	// var sparkJobs []SparkJob
	// var sparkApp SparkApp
	var messageState = "NONE"

	var startMessage = "Hey @dacort! A new Spark app is starting...! 💁‍♂️ %s\n\nID: %s"

	go func() {
		// defer wg.Done()
		startTime := time.Now()

		// Try to populate the Spark app before we go ticking along
		appInfo, err := getSparkApp()
		if err == nil {
			tweeterChannel <- fmt.Sprintf(startMessage, appInfo.Name, appInfo.Id)
			messageState = "INIT"
		}

		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				appInfo, err := getSparkApp()
				if err != nil {
					fmt.Println("Couldn't fetch Spark info")
					continue
				}
				if messageState == "NONE" {
					tweeterChannel <- fmt.Sprintf(startMessage, appInfo.Name, appInfo.Id)
					messageState = "INIT"
				} else if messageState == "INIT" && time.Now().Sub(startTime).Minutes() >= 1 {
					sparkJobs, err := getSparkJobs(appInfo.Id)
					if err != nil {
						fmt.Println("Couldn't fetch Spark Jobs", err)
					} else {
						active, completed := countJobs(*sparkJobs)
						tweeterChannel <- fmt.Sprintf("OK, one minute in and still chugging...\nJob status: %d active / %d completed", active, completed)
						messageState = "UPDATE_1"
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
