package main

import (
	// other imports
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

const INITIAL_HEARTBEAT_TIMEOUT_SEC = 60 // We wait at least 60 seconds for the Spark driver to start
const HEARTBEAT_TIMEOUT_SEC = 15         // We wait at least 15 seconds from the last heartbeat before shutting down

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

	// We keep track of tweetIDs here in order to create a thread
	var tweetID int64

	go func() {
		for {
			statusText := <-tweetText
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

type SparkAttempt struct {
	SparkUser string `json:"sparkUser"`
	StartTime string `json:"startTime"`
}
type SparkApp struct {
	Id       string         `json:"id"`
	Name     string         `json:"name"`
	Attempts []SparkAttempt `json:"attempts"`
}

type SparkJob struct {
	JobID             int    `json:"jobId"`
	Status            string `json:"status"`
	NumTasks          int    `json:"numTasks"`
	NumActiveTasks    int    `json:"numActiveTasks"`
	NumCompletedTasks int    `json:"numCompletedTasks"`
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

func parseSparkJobs(body []byte) (*[]SparkJob, error) {
	var jobs []SparkJob
	err := json.Unmarshal(body, &jobs)
	if err != nil {
		fmt.Println("whoops:", err)
		return nil, err
	}

	// We only ever have one app
	return &jobs, err
}

func getSparkJobs(app_id string) (*[]SparkJob, error) {
	jobsEndpoint := fmt.Sprintf("http://localhost:4040/api/v1/applications/%s/jobs", app_id)
	resp, err := http.Get(jobsEndpoint)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	j, err := parseSparkJobs(body)
	return j, err
}

/*
	EMR stores a heartbeat file on a shared volume.
	For more details see https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/pod-templates.html#pod-template-sidecar
*/
func heatbeatFile() string {
	return filepath.Join(os.Getenv("EMR_COMMS_MOUNT"), "main-container-terminated")
}

func waitForHeartbeatInit() error {
	log.Printf("Waiting %ds for EMR Spark heartbeat at '%s'\n", INITIAL_HEARTBEAT_TIMEOUT_SEC, heatbeatFile())
	t1 := time.Now()
	for range time.Tick(1 * time.Second) {
		if _, err := os.Stat(heatbeatFile()); err == nil {
			return nil
		}
		t2 := time.Now()
		if t2.Sub(t1).Seconds() > INITIAL_HEARTBEAT_TIMEOUT_SEC {
			return fmt.Errorf("Timeout while waiting for Spark heartbeat")
		}
	}

	return nil
}

func waitForFlatline(cancel context.CancelFunc) {
	/*
		Wait for the EMR heartbeat file to stop updating.
		It should be noted that the file will actually disappear eventually, so we need to
		keep track of the last heartbeat in a separate variable and can't rely on the os.Stat.
	*/
	lastHeartbeat := time.Now()
	for range time.Tick(5 * time.Second) {
		f, err := os.Stat(heatbeatFile())
		if err == nil {
			lastHeartbeat = f.ModTime()
		}
		if time.Now().Sub(lastHeartbeat).Seconds() > HEARTBEAT_TIMEOUT_SEC {
			cancel()
			break
		}
	}
}

func countJobs(jobs []SparkJob) (active int, completed int) {
	for _, j := range jobs {
		active += j.NumActiveTasks
		completed += j.NumCompletedTasks
	}

	return
}

func sparkAPIMonitor(ctx context.Context, wg *sync.WaitGroup, tweeterChannel chan string) {
	// var sparkJobs []SparkJob
	// var sparkApp SparkApp
	var messageState = "NONE"

	var startMessage = "Hey @dacort! A new Spark app is starting...! 💁‍♂️ %s\n\nID: %s"

	go func() {
		defer wg.Done()
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
				fmt.Println("SparkAPIMon checking in...", messageState)
				appInfo, err := getSparkApp()
				if err != nil {
					fmt.Println("Couldn't fetch Spark info")
					continue
				}
				if messageState == "NONE" {
					tweeterChannel <- fmt.Sprintf(startMessage, appInfo.Name, appInfo.Id)
					messageState = "INIT"
				} else if messageState == "INIT" && time.Now().Sub(startTime).Minutes() >= 1 {
					fmt.Println("Trying to get Spark jobs")
					sparkJobs, err := getSparkJobs(appInfo.Id)
					fmt.Println(sparkJobs)
					if err != nil {
						fmt.Println("Couldn't fetch Spark Jobs", err)
					} else {
						active, completed := countJobs(*sparkJobs)
						fmt.Println(active, completed)
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

func main() {
	fmt.Println("Go-Twitter Bot v0.01")

	// First, wait for the main-container-terminated file to show up
	err := waitForHeartbeatInit()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Then, we start tweet channel
	//       and a Spark API channel monitor
	// 		 and a main-container-terminated channel

	// Create a waitGroup
	// Create a channel we can send tweet updates to
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

	// Set up cancellation context and waitgroup
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	sparkAPIMonitor(ctx, wg, tweeterChannel)
	wg.Add(1)
	// Then start a watcher for the heartbeat file, it can call cancelFunc
	// Start a watcher for spark info and that'll tweet stuff out
	// Start a watcher for stdout, and that'll be the last tweet
	waitForFlatline(cancelFunc)
	wg.Wait()

	tweeterChannel <- "We're all done! 👋"

	// t, err := tail.TailFile(filepath.Join(sparkDriverPath, "stdout"), tail.Config{Follow: true, ReOpen: true})
	// if err != nil {
	// 	log.Fatalln("Couldn't tail file", err)
	// }
	// for line := range t.Lines {
	// 	fmt.Println(line.Text)
	// }
}
