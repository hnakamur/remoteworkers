package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type WorkerResult struct {
	JobID JobID
	Data  interface{}
}

var (
	_ msgpack.CustomEncoder = &WorkerResult{}
	_ msgpack.CustomDecoder = &WorkerResult{}
)

func (r *WorkerResult) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.JobID, r.Data)
}

func (r *WorkerResult) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.JobID, &r.Data)
}
