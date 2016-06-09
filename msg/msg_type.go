package msg

type MessageType uint32

const (
	UndefinedMsg            MessageType = 0
	RegisterWorkerResultMsg MessageType = 1
	JobMsg                  MessageType = 2
	WorkerResultMsg         MessageType = 3
)
