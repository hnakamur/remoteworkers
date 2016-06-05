package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type TargetResult struct {
	Target string
	Result string
}

var (
	_ msgpack.CustomEncoder = &TargetResult{}
	_ msgpack.CustomDecoder = &TargetResult{}
)

func (r *TargetResult) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.Target, r.Result)
}

func (r *TargetResult) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.Target, &r.Result)
}
