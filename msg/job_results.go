package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type JobResults struct {
	JobID   uint64
	Results []JobResult
}

var (
	_ msgpack.CustomEncoder = &JobResults{}
	_ msgpack.CustomDecoder = &JobResults{}
)

func (r *JobResults) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.JobID, r.Results)
}

func (r *JobResults) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.JobID, &r.Results)
}
