package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type RegisterWorker struct {
	WorkerID uint64
}

var (
	_ msgpack.CustomEncoder = &RegisterWorker{}
	_ msgpack.CustomDecoder = &RegisterWorker{}
)

func (r *RegisterWorker) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.WorkerID)
}

func (r *RegisterWorker) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.WorkerID)
}

type RegisterWorkerResult struct {
	Registered bool
}

var (
	_ msgpack.CustomEncoder = &RegisterWorkerResult{}
	_ msgpack.CustomDecoder = &RegisterWorkerResult{}
)

func (r *RegisterWorkerResult) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.Registered)
}

func (r *RegisterWorkerResult) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.Registered)
}
