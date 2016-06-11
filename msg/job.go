package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type Job struct {
	ID     uint64      `json:"-"`
	Params interface{} `json:"params,omitempty"`
}

var (
	_ msgpack.CustomEncoder = &Job{}
	_ msgpack.CustomDecoder = &Job{}
)

func (j *Job) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(j.ID, j.Params)
}

func (j *Job) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&j.ID, &j.Params)
}
