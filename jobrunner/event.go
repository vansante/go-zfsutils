package jobrunner

import eventemitter "github.com/vansante/go-event-emitter"

const (
	CreatedSnapshotEvent   eventemitter.EventType = "created-snapshot"
	SentSnapshotEvent      eventemitter.EventType = "sent-snapshot"
	DeletedSnapshotEvent   eventemitter.EventType = "deleted-snapshot"
	DeletedFilesystemEvent eventemitter.EventType = "deleted-filesystem"
)
