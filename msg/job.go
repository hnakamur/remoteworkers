package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type JobID uint64

type Job struct {
	ID     JobID       `json:"-"`
	Type   string      `json:"type"`
	Params interface{} `json:"params,omitempty"`
}

var (
	_ msgpack.CustomEncoder = &Job{}
	_ msgpack.CustomDecoder = &Job{}
)

func (j *Job) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(j.ID, j.Type, j.Params)
}

func (j *Job) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&j.ID, &j.Type, &j.Params)
}
