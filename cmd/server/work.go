package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"bitbucket.org/hnakamur/ws_surveyor/msg"

	"github.com/hnakamur/ltsvlog"
)

type workRequest struct {
	Params interface{} `json:"params"`
}

type workResponse struct {
	JobID   string                 `json:"job_id"`
	Results map[string]interface{} `json:"results"`
}

func newWorkResponse(jobID msg.JobID, results map[string]interface{}) *workResponse {
	workRes := &workResponse{
		JobID:   strconv.FormatUint(uint64(jobID), 10),
		Results: make(map[string]interface{}),
	}
	for workerID, r := range results {
		data := make(map[string]bool)
		for k, v := range r.(map[interface{}]interface{}) {
			data[k.(string)] = v.(bool)
		}
		workRes.Results[workerID] = data
	}
	return workRes
}

func serveWork(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/work" {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	dec := json.NewDecoder(r.Body)
	var v workRequest
	err := dec.Decode(&v)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	results, jobID, err := hub.RequestWork(v.Params)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "error returned from worker hub"},
			ltsvlog.LV{"err", err})
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	workResp := newWorkResponse(jobID, results)
	enc := json.NewEncoder(w)
	err = enc.Encode(&workResp)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to encode work response"},
			ltsvlog.LV{"err", err})
	}
}
