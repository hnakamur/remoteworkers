package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/hnakamur/ltsvlog"
)

type workRequest struct {
	Params map[string]string `json:"params"`
}

func main() {
	var baseURL string
	flag.StringVar(&baseURL, "base-url", "http://localhost:8080", "base URL")
	flag.Parse()

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	workReq := &workRequest{
		Params: map[string]string{
			"targets": "targets1,targets2",
		},
	}
	err := enc.Encode(&workReq)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to encode work request"},
			ltsvlog.LV{"err", err})
	}
	u := fmt.Sprintf("%s/work", baseURL)
	ltsvlog.Logger.Info(ltsvlog.LV{"msg", "sending request"}, ltsvlog.LV{"workReq", *workReq})
	resp, err := http.Post(u, "application/json", &buf)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to receive response"},
			ltsvlog.LV{"err", err})
	}
	defer resp.Body.Close()

	buf.Reset()
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read response body"},
			ltsvlog.LV{"err", err})
	}
	ltsvlog.Logger.Info(ltsvlog.LV{"msg", "received response"}, ltsvlog.LV{"resp", strings.TrimSpace(buf.String())})
}
