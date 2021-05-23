package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

const INITIAL_HEARTBEAT_TIMEOUT_SEC = 60 // We wait at least 60 seconds for the Spark driver to start
const HEARTBEAT_TIMEOUT_SEC = 15         // We wait at least 15 seconds from the last heartbeat before shutting down

// EMR stores a heartbeat file on a shared volume.
// For more details see https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/pod-templates.html#pod-template-sidecar
func heatbeatFile() string {
	return filepath.Join(os.Getenv("EMR_COMMS_MOUNT"), "main-container-terminated")
}

// Wait for `INITIAL_HEARTBEAT_TIMEOUT_SEC` seconds for the heartbeat file to show up.
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

// Wait for the EMR heartbeat file to stop updating.
// It should be noted that the file will actually disappear eventually, so we need to
// keep track of the last heartbeat in a separate variable and can't rely on the os.Stat.
func waitForFlatline(cancel context.CancelFunc) {
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
