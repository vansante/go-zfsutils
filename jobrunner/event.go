package jobrunner

import eventemitter "github.com/vansante/go-event-emitter"

const (
	CreatedSnapshotEvent      eventemitter.EventType = "created-snapshot"
	SendingSnapshotEvent      eventemitter.EventType = "sending-snapshot"
	SentSnapshotEvent         eventemitter.EventType = "sent-snapshot"
	MarkSnapshotDeletionEvent eventemitter.EventType = "mark-snapshot-deletion"
	DeletedSnapshotEvent      eventemitter.EventType = "deleted-snapshot"
	DeletedFilesystemEvent    eventemitter.EventType = "deleted-filesystem"
)
