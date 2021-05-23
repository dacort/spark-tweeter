package main

import (
	// other imports
	"context"
	"fmt"
	"log"
	"sync"
)

func main() {
	fmt.Println("Go-Twitter Bot v0.01")

	// First, wait for the main-container-terminated file to show up.
	// We exist if that doesn't happen in time.
	err := waitForHeartbeatInit()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Set up cancellation context and waitgroup
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Create a channel we can send tweet updates to.
	// This gets passed around to different routines, but we close it in the main thread.
	tweeterChannel := tweetMonitor(ctx, wg)

	// Keep an eye on our job progress and tweet some pithy comments about how slow it is.
	sparkAPIMonitor(ctx, wg, tweeterChannel)

	// Then start a watcher for the heartbeat file, it can call cancelFunc
	waitForFlatline(cancelFunc)

	// That's it, folks! send out a final tweet...close that channel and go home.
	tweeterChannel <- "We're all done! 👋"
	close(tweeterChannel)
	wg.Wait()
}
