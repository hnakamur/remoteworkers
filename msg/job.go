package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type Job struct {
	JobID   uint64
	Targets []string
}

var (
	_ msgpack.CustomEncoder = &Job{}
	_ msgpack.CustomDecoder = &Job{}
)

func (j *Job) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(j.JobID, j.Targets)
}

func (j *Job) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&j.JobID, &j.Targets)
}
