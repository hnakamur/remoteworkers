// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"net/http"
	"time"

	"bitbucket.org/hnakamur/ws_surveyor/msg"
	"github.com/gorilla/websocket"
	"github.com/hnakamur/ltsvlog"
	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512

	// WorkerID header name
	WorkerIDHeaderName = "X-Worker-ID"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Conn is an middleman between the websocket connection and the hub.
type Conn struct {
	// The websocket connection.
	ws *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte

	// Worker ID
	workerID string
}

// readPump pumps messages from the websocket connection to the hub.
func (c *Conn) readPump() {
	defer func() {
		hub.unregisterWorkerC <- c
		c.ws.Close()
	}()
	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error { c.ws.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		wsMsgType, r, err := c.ws.NextReader()
		if err != nil {
			ltsvlog.Logger.Error(ltsvlog.LV{"msg", "read error"},
				ltsvlog.LV{"err", err})
			return
		}
		if wsMsgType != websocket.BinaryMessage {
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected wsMsgType"},
				ltsvlog.LV{"wsMsgType", wsMsgType})
			return
		}

		dec := msgpack.NewDecoder(r)
		var msgType msg.MessageType
		err = dec.Decode(&msgType)
		if err != nil {
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
				ltsvlog.LV{"err", err})
			return
		}
		switch msgType {
		case msg.WorkerResultMsg:
			var res msg.WorkerResult
			err := dec.Decode(&res)
			if err != nil {
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
					ltsvlog.LV{"err", err})
				return
			}

			ltsvlog.Logger.Info(ltsvlog.LV{"msg", "received WorkerResult"},
				ltsvlog.LV{"workerID", c.workerID},
				ltsvlog.LV{"workerResult", res})

			hub.workerResultToHubC <- workerResult{
				workerID: c.workerID,
				result:   &res,
			}
		default:
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
				ltsvlog.LV{"messageType", msgType})
			return
		}
	}
}

// write writes a message with the given message type and payload.
func (c *Conn) write(mt int, payload []byte) error {
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteMessage(mt, payload)
}

// writePump pumps messages from the hub to the websocket connection.
func (c *Conn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.write(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.write(websocket.BinaryMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.write(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

// serveWs handles websocket requests from the peer.
func serveWs(w http.ResponseWriter, r *http.Request) {
	workerID := r.Header.Get(WorkerIDHeaderName)
	if workerID == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to upgrade to webscoket"},
			ltsvlog.LV{"err", err})
		return
	}
	conn := &Conn{send: make(chan []byte, 256), ws: ws, workerID: workerID}

	registeredC := make(chan bool)
	req := registerWorkerRequest{
		conn:    conn,
		resultC: registeredC,
	}
	hub.registerWorkerC <- req
	registered := <-registeredC
	var registerWorkerResult msg.RegisterWorkerResult
	if !registered {
		registerWorkerResult.Error = "woker with same name already exists"
	}
	message, err := msgpack.Marshal(msg.RegisterWorkerResultMsg, &registerWorkerResult)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
			ltsvlog.LV{"registerWorkerResult", registerWorkerResult},
			ltsvlog.LV{"err", err})
		close(conn.send)
		return
	}
	conn.send <- message

	go conn.writePump()
	conn.readPump()
}
