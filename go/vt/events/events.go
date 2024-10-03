package events

type SourceType string

const (
	SourceVtctld SourceType = "vtctld"
	SourceVtorc  SourceType = "vtorc"
)

type Source struct {
	Type     SourceType `json:"type"`
	Hostname string     `json:"hostname"`
}

func NewSourceVtctld(hostname string) Source {
	return Source{
		Type:     SourceVtctld,
		Hostname: hostname,
	}
}
