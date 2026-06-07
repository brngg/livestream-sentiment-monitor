package chat

import "context"

type Reader interface {
	Read(ctx context.Context) (<-chan ChatMessage, <-chan error)
}
