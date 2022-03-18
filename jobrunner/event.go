package jobrunner

import eventemitter "github.com/vansante/go-event-emitter"

const (
	CreatedSnapshot   eventemitter.EventType = "created-snapshot"
	SentSnapshot      eventemitter.EventType = "sent-snapshot"
	DeletedSnapshot   eventemitter.EventType = "deleted-snapshot"
	DeletedFilesystem eventemitter.EventType = "deleted-filesystem"
)
