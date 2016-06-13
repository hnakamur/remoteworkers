package remoteworkers

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

type jobMessage struct {
	ID     uint64      `json:"-"`
	Params interface{} `json:"params,omitempty"`
}

type workerResultMessage struct {
	JobID uint64
	Data  interface{}
}
