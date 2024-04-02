package job

import eventemitter "github.com/vansante/go-event-emitter"

const (
	CreatedSnapshotEvent         eventemitter.EventType = "created-snapshot"
	StartSendingSnapshotEvent    eventemitter.EventType = "start-sending-snapshot"
	SnapshotSendingProgressEvent eventemitter.EventType = "snapshot-sending-progress"
	ResumeSendingSnapshotEvent   eventemitter.EventType = "resume-sending-snapshot"
	SentSnapshotEvent            eventemitter.EventType = "sent-snapshot"
	MarkSnapshotDeletionEvent    eventemitter.EventType = "mark-snapshot-deletion"
	DeletedSnapshotEvent         eventemitter.EventType = "deleted-snapshot"
	DeletedFilesystemEvent       eventemitter.EventType = "deleted-filesystem"
)
