package jobrunner

type JobType string

const (
	SnapshotJob JobType = "local-snapshot"
	PullJob     JobType = "pull"
	PushJob     JobType = "push"
)

type Job struct {
	Server           string
	AuthToken        string
	Port             int64
	LocalFilesystem  string
	RemoteFilesystem string
	Snapshot         string
}

type Push struct {
	Job
}

type Pull struct {
	Job
}
