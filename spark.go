package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

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

func countJobs(jobs []SparkJob) (active int, completed int) {
	for _, j := range jobs {
		active += j.NumActiveTasks
		completed += j.NumCompletedTasks
	}

	return
}
