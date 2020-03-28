package outbox

type Event struct {
	ID        string
	Publisher string
	EventName string
	Timestamp int64
	Payload   []byte
}

func (e Event) GetID() string {
	return e.ID
}

func (e Event) GetPublisher() string {
	return e.Publisher
}

func (e Event) GetTimestamp() int64 {
	return e.Timestamp
}

func (e Event) GetEventName() string {
	return e.EventName
}

func (e Event) GetPayload() []byte {
	return e.Payload
}
