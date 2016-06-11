package ws_surveyor

import (
	"time"

	"bitbucket.org/hnakamur/ws_surveyor/msg"
	"github.com/gorilla/websocket"
	"github.com/hnakamur/ltsvlog"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type ConnConfig struct {
	// SendChannelLen is length of send channel.
	SendChannelLen int

	// WriteWait is time allowed to write a message to the worker.
	WriteWait time.Duration

	// PongWait is time allowed to read the next pong message from the worker.
	PongWait time.Duration

	// PingPeriod is period which the connection pings to worker with. Must be less than PongWait.
	PingPeriod time.Duration

	// Maximum message size allowed from worker.
	MaxMessageSize int64
}

func DefaultConnConfig() ConnConfig {
	pongWait := 60 * time.Second
	return ConnConfig{
		SendChannelLen: 256,
		WriteWait:      10 * time.Second,
		PongWait:       pongWait,
		PingPeriod:     (pongWait * 9) / 10,
		MaxMessageSize: 512,
	}
}

// Conn is an middleman between the websocket connection and the hub.
type Conn struct {
	// The hub.
	hub *Hub

	// The websocket connection.
	ws *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte

	// Worker ID.
	workerID string

	// writeWait is time allowed to write a message to the worker.
	writeWait time.Duration

	// pongWait is time allowed to read the next pong message from the worker.
	pongWait time.Duration

	// pingPeriod is period which the connection pings to worker with. Must be less than PongWait.
	pingPeriod time.Duration

	// maximum message size allowed from worker.
	maxMessageSize int64
}

func NewConn(ws *websocket.Conn, workerID string, config ConnConfig) *Conn {
	return &Conn{
		ws:             ws,
		send:           make(chan []byte, config.SendChannelLen),
		workerID:       workerID,
		writeWait:      config.WriteWait,
		pongWait:       config.PongWait,
		pingPeriod:     config.PingPeriod,
		maxMessageSize: config.MaxMessageSize,
	}
}

func (c *Conn) RegisterToHub(h *Hub) error {
	registeredC := make(chan bool)
	req := registerWorkerRequest{
		conn:    c,
		resultC: registeredC,
	}
	h.registerWorkerC <- req
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
		close(c.send)
		return err
	}
	c.hub = h
	c.send <- message
	return nil
}

func (c *Conn) Run() {
	go c.writePump()
	c.readPump()
}

// readPump pumps messages from the websocket connection to the hub.
func (c *Conn) readPump() {
	defer func() {
		c.hub.unregisterWorkerC <- c
		c.ws.Close()
	}()
	c.ws.SetReadLimit(c.maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(c.pongWait))
	c.ws.SetPongHandler(func(string) error { c.ws.SetReadDeadline(time.Now().Add(c.pongWait)); return nil })
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

			c.hub.workerResultToHubC <- workerResult{
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
	c.ws.SetWriteDeadline(time.Now().Add(c.writeWait))
	return c.ws.WriteMessage(mt, payload)
}

// writePump pumps messages from the c.hub to the websocket connection.
func (c *Conn) writePump() {
	ticker := time.NewTicker(c.pingPeriod)
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
