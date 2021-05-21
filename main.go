package main

import (
	// other imports
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/nxadm/tail"
)

// Credentials stores all of our access/consumer tokens
// and secret keys needed for authentication against
// the twitter REST API.
type Credentials struct {
	ConsumerKey       string
	ConsumerSecret    string
	AccessToken       string
	AccessTokenSecret string
}

// getClient is a helper function that will return a twitter client
// that we can subsequently use to send tweets, or to stream new tweets
// this will take in a pointer to a Credential struct which will contain
// everything needed to authenticate and return a pointer to a twitter Client
// or an error
func getClient(creds *Credentials) (*twitter.Client, error) {
	// Pass in your consumer key (API Key) and your Consumer Secret (API Secret)
	config := oauth1.NewConfig(creds.ConsumerKey, creds.ConsumerSecret)
	// Pass in your Access Token and your Access Token Secret
	token := oauth1.NewToken(creds.AccessToken, creds.AccessTokenSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	client := twitter.NewClient(httpClient)

	// Verify Credentials
	verifyParams := &twitter.AccountVerifyParams{
		SkipStatus:   twitter.Bool(true),
		IncludeEmail: twitter.Bool(true),
	}

	// we can retrieve the user and verify if the credentials
	// we have used successfully allow us to log in!
	user, _, err := client.Accounts.VerifyCredentials(verifyParams)
	if err != nil {
		return nil, err
	}

	log.Printf("User's ACCOUNT: %s\n", user.ScreenName)
	return client, nil
}

func getTwitterClient() *twitter.Client {
	creds := Credentials{
		AccessToken:       os.Getenv("ACCESS_TOKEN"),
		AccessTokenSecret: os.Getenv("ACCESS_TOKEN_SECRET"),
		ConsumerKey:       os.Getenv("CONSUMER_KEY"),
		ConsumerSecret:    os.Getenv("CONSUMER_SECRET"),
	}

	client, err := getClient(&creds)
	if err != nil {
		log.Println("Error getting Twitter Client")
		log.Fatalln(err)
	}

	return client
}

func tweeter() chan string {
	client := getTwitterClient()
	tweetText := make(chan string)

	go func() {
		for {
			tweet := <-tweetText

			_, _, err := client.Statuses.Update(tweet, nil)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	return tweetText
}

type SparkAttempt struct {
	SparkUser string `json:"sparkUser"`
	StartTime string `json:"startTime"`
}
type SparkApp struct {
	Id       string         `json:"id"`
	Name     string         `json:"name"`
	Attempts []SparkAttempt `json:"attempts":`
}

func parseSparkApp(body []byte) (*SparkApp, error) {
	var apps []SparkApp
	err := json.Unmarshal(body, &apps)
	if err != nil {
		fmt.Println("whoops:", err)
		return nil, err
	}

	// We only ever have one app
	return &apps[0], err
}

func getSparkApp() (*SparkApp, error) {
	resp, err := http.Get("http://localhost:4040/api/v1/applications")
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	s, err := parseSparkApp(body)
	return s, err
}

func main() {
	fmt.Println("Go-Twitter Bot v0.01")

	tweeterChannel := tweeter()

	fmt.Println("Waiting for log files to show up...")
	var sparkDriverPath string
	for {
		files, _ := filepath.Glob("/var/log/spark/user/spark*driver")
		if len(files) > 0 {
			sparkDriverPath = files[0]
			fmt.Println("Found our spark driver logs!", sparkDriverPath)

			break
		} else {
			fmt.Println("No files found..")
		}
		time.Sleep(5 * time.Second)
	}

	// Now we get the spark app info
	appInfo, err := getSparkApp()
	if err != nil {
		fmt.Println("Couldn't fetch Spark info")
	} else {
		tweeterChannel <- fmt.Sprintf("A new Spark app has been created! 💁‍♂️ %s", appInfo.Name)
	}

	t, err := tail.TailFile(filepath.Join(sparkDriverPath, "stderr"), tail.Config{Follow: true, ReOpen: true})
	if err != nil {
		log.Fatalln("Couldn't tail file", err)
	}
	for line := range t.Lines {
		fmt.Println(line.Text)
	}
}
