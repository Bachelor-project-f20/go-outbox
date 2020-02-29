package outbox

type EventEmitter interface {
	Emit(e Event) error
}
