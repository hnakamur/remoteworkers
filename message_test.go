package remoteworkers

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func TestEncodeDecodeJob(t *testing.T) {
	var buf bytes.Buffer

	targets := "target1,target2"
	job1 := jobMessage{
		Params: map[string]string{
			"targets": targets,
		},
	}

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(jobMsg)
	if err != nil {
		t.Error(err)
	}
	gob.Register(make(map[string]string))
	err = enc.Encode(&job1)
	if err != nil {
		t.Error(err)
	}

	dec := gob.NewDecoder(&buf)
	var msgType messageType
	err = dec.Decode(&msgType)
	if err != nil {
		t.Error(err)
	}
	var job2 jobMessage
	err = dec.Decode(&job2)
	if err != nil {
		t.Error(err)
	}
	if msgType != jobMsg {
		t.Errorf("unexpected msgType. got=%d, want=%d", msgType, jobMsg)
	}
	decodedTargets := job2.Params.(map[string]string)["targets"]
	if decodedTargets != targets {
		t.Errorf("unexpected targets. got=%d, want=%d", decodedTargets, targets)
	}
}
