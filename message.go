package remoteworkers

import "gopkg.in/vmihailenco/msgpack.v2"

type messageType uint32

const (
	undefinedMsg            messageType = 0
	registerWorkerResultMsg messageType = 1
	jobMsg                  messageType = 2
	workerResultMsg         messageType = 3
)

type typeAndMessage struct {
	Type    messageType
	Message interface{}
}

type registerWorkerResultMessage struct {
	Error string
}

var (
	_ msgpack.CustomEncoder = &registerWorkerResultMessage{}
	_ msgpack.CustomDecoder = &registerWorkerResultMessage{}
)

func (r *registerWorkerResultMessage) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.Error)
}

func (r *registerWorkerResultMessage) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.Error)
}

type jobMessage struct {
	ID     uint64      `json:"-"`
	Params interface{} `json:"params,omitempty"`
}

var (
	_ msgpack.CustomEncoder = &jobMessage{}
	_ msgpack.CustomDecoder = &jobMessage{}
)

func (j *jobMessage) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(j.ID, j.Params)
}

func (j *jobMessage) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&j.ID, &j.Params)
}

type workerResultMessage struct {
	JobID uint64
	Data  interface{}
}

var (
	_ msgpack.CustomEncoder = &workerResultMessage{}
	_ msgpack.CustomDecoder = &workerResultMessage{}
)

func (r *workerResultMessage) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.JobID, r.Data)
}

func (r *workerResultMessage) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.JobID, &r.Data)
}
