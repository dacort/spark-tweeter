package main

import (
	// other imports
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)

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
