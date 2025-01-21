package vtgateproxy

type RotorJson struct {
	VersionInfo  string       `json:"version_info"`
	Resources    []Resource   `json:"resources"`
	TypeURL      string       `json:"type_url"`
	ControlPlane ControlPlane `json:"control_plane"`
}

type ControlPlane struct {
	Identifier string `json:"identifier"`
}

type Resource struct {
	Type        string            `json:"@type"`
	ClusterName string            `json:"cluster_name"`
	Endpoints   []EndpointElement `json:"endpoints"`
}

type EndpointElement struct {
	LBEndpoints []LBEndpoint `json:"lb_endpoints"`
}

type LBEndpoint struct {
	Endpoint     LBEndpointEndpoint `json:"endpoint"`
	HealthStatus HealthStatus       `json:"health_status"`
	Metadata     Metadata           `json:"metadata"`
}

type LBEndpointEndpoint struct {
	Address Address `json:"address"`
}

type Address struct {
	SocketAddress SocketAddress `json:"socket_address"`
}

type SocketAddress struct {
	Address   string `json:"address"`
	PortValue int64  `json:"port_value"`
}

type Metadata struct {
	FilterMetadata FilterMetadata `json:"filter_metadata"`
}

type FilterMetadata struct {
	EnvoyLB EnvoyLB `json:"envoy.lb"`
}

type EnvoyLB struct {
	Consul                   Consul            `json:"consul"`
	NodeAddress              string            `json:"node-address"`
	NodeHealth               NodeHealth        `json:"node-health"`
	NodeID                   string            `json:"node-id"`
	NodeName                 string            `json:"node-name"`
	NodeASG                  ASG               `json:"node:asg"`
	NodeAz                   NodeAz            `json:"node:az"`
	NodeAzID                 AzID              `json:"node:az_id"`
	NodeConsulNetworkSegment string            `json:"node:consul-network-segment"`
	NodeConsulVersion        ConsulVersion     `json:"node:consul-version"`
	NodeEnv                  Env               `json:"node:env"`
	NodeLSBRelease           string            `json:"node:lsb_release"`
	NodeNebulaAddress        string            `json:"node:nebula_address"`
	NodeOmniServiceID        string            `json:"node:omni_service_id"`
	NodePlatform             Platform          `json:"node:platform"`
	NodeProvider             Provider          `json:"node:provider"`
	NodeRegion               Region            `json:"node:region"`
	NodeRole                 Role              `json:"node:role"`
	TagDatacenter            TagDatacenterEnum `json:"tag:datacenter"`
	TagLegacyDc              TagDatacenterEnum `json:"tag:legacy_dc"`
}

type Consul struct {
	NodeMeta NodeMeta `json:"nodeMeta"`
	Tag      Tag      `json:"tag"`
}

type NodeMeta struct {
	ASG                  ASG           `json:"asg"`
	Az                   NodeAz        `json:"az"`
	AzID                 AzID          `json:"az_id"`
	ConsulNetworkSegment string        `json:"consul-network-segment"`
	ConsulVersion        ConsulVersion `json:"consul-version"`
	Env                  Env           `json:"env"`
	LSBRelease           string        `json:"lsb_release"`
	NebulaAddress        string        `json:"nebula_address"`
	OmniServiceID        string        `json:"omni_service_id"`
	Platform             Platform      `json:"platform"`
	Provider             Provider      `json:"provider"`
	Region               Region        `json:"region"`
	Role                 Role          `json:"role"`
}

type Tag struct {
	DatacenterDevUsEast1 string `json:"datacenter:dev-us-east-1"`
	LegacyDcDevUsEast1   string `json:"legacy_dc:dev-us-east-1"`
}

type HealthStatus string

const (
	Healthy HealthStatus = "HEALTHY"
)

type ASG string

const (
	ASGRdevCanvasWebapp               ASG = "rdev-canvas-webapp"
	DevContainersPoolLargeWhitecastle ASG = "dev-containers-pool-large-whitecastle"
	DevContainersPoolTestWhitecastle  ASG = "dev-containers-pool-test-whitecastle"
	DevContainersPoolWhitecastle      ASG = "dev-containers-pool-whitecastle"
	Empty                             ASG = ""
	QAEnvContainerPoolWc              ASG = "qa-env-container-pool-wc"
	QAEnvSandboxContainerPoolWc       ASG = "qa-env-sandbox-container-pool-wc"
)

type NodeAz string

const (
	UsEast1A NodeAz = "us-east-1a"
	UsEast1B NodeAz = "us-east-1b"
	UsEast1C NodeAz = "us-east-1c"
	UsEast1D NodeAz = "us-east-1d"
)

type AzID string

const (
	Use1Az1 AzID = "use1-az1"
	Use1Az2 AzID = "use1-az2"
	Use1Az4 AzID = "use1-az4"
	Use1Az6 AzID = "use1-az6"
)

type ConsulVersion string

const (
	The1201Slack16 ConsulVersion = "1.20.1+slack.16"
)

type Env string

const (
	Dev Env = "dev"
)

type Platform string

const (
	Chef Platform = "chef"
)

type Provider string

const (
	Ec2 Provider = "ec2"
)

type Region string

const (
	UsEast1 Region = "us-east-1"
)

type Role string

const (
	RoleRdevCanvasWebapp       Role = "rdev-canvas-webapp"
	SlackDevContainer          Role = "slack-dev-container"
	SlackQAEnvContainer        Role = "slack-qa-env-container"
	SlackQAEnvSandboxContainer Role = "slack-qa-env-sandbox-container"
)

type NodeHealth string

const (
	Passing NodeHealth = "passing"
)

type TagDatacenterEnum string

const (
	DevUsEast1 TagDatacenterEnum = "dev-us-east-1"
)
