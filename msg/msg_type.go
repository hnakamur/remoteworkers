package msg

type MessageType uint32

const (
	UndefinedMsg            MessageType = 0
	JobMsg                  MessageType = 1
	RegisterWorkerResultMsg MessageType = 2
	WorkerResultMsg         MessageType = 3
)
