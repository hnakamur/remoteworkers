package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"bitbucket.org/hnakamur/ws_surveyor/msg"

	"github.com/hnakamur/ltsvlog"
)

type workRequest struct {
	Type   string      `json:"type"`
	Params interface{} `json:"params,omitempty"`
}

type workResponse struct {
	JobID   string                 `json:"job_id"`
	Results map[string]interface{} `json:"results"`
}

func newWorkResponse(res jobResult) *workResponse {
	workRes := &workResponse{
		Results: make(map[string]interface{}),
	}
	for workerID, r := range res.results {
		data := make(map[string]bool)
		if workRes.JobID == "" {
			workRes.JobID = strconv.FormatUint(uint64(r.JobID), 10)
		}
		for k, v := range r.Data.(map[interface{}]interface{}) {
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

	job := msg.Job{
		Type:   v.Type,
		Params: v.Params,
	}
	resultC := make(chan jobResultOrError)
	hub.broadcastToWorkersC <- jobRequestToHub{
		job:     job,
		resultC: resultC,
	}

	resOrErr := <-resultC
	if resOrErr.err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "error returned from worker hub"},
			ltsvlog.LV{"err", resOrErr.err})
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	workResp := newWorkResponse(resOrErr.result)
	enc := json.NewEncoder(w)
	err = enc.Encode(&workResp)
	if err != nil {
		log.Println(err)
	}
}
