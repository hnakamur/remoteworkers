package msg

type MessageType uint32

const (
	UndefinedMsg            MessageType = 0
	JobMsg                  MessageType = 1
	JobResultMsg            MessageType = 2
	JobResultsMsg           MessageType = 3
	RegisterWorkerMsg       MessageType = 4
	RegisterWorkerResultMsg MessageType = 5
)
