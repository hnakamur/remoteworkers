package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type RegisterWorkerResult struct {
	Error string
}

var (
	_ msgpack.CustomEncoder = &RegisterWorkerResult{}
	_ msgpack.CustomDecoder = &RegisterWorkerResult{}
)

func (r *RegisterWorkerResult) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.Error)
}

func (r *RegisterWorkerResult) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.Error)
}
