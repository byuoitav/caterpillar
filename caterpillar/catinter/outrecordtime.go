package catinter

import "time"

//recordTypes
const (
	Input  = "input"
	Power  = "power"
	Blank  = "blank"
	Volume = "volume"
	Mute   = "mute"
)

//MetricsRecord .
type MetricsRecord struct {
	StartTime        time.Time `json:"start-time,omitempty"`
	EndTime          time.Time `json:"end-time,omitempty"`
	ElapsedInSeconds int64     `json:"elapsed-in-seconds,omitempty"`
	RecordType       string    `json:"record-type,omitempty"`

	Device DeviceInfo `json:"device,omitempty"`
	Room   RoomInfo   `json:"room,omitempty"`

	Input     string `json:"input,omitempty"`
	InputType string `json:"input-type,omitempty"`

	Volume  *int   `json:"volume,omitempty"`
	Blanked *bool  `json:"blanked,omitempty"`
	Muted   *bool  `json:"muted,omitempty"`
	Power   string `json:"power,omitempty"`
}

//DeviceInfo .
type DeviceInfo struct {
	ID          string   `json:"id,omitempty"`
	DeviceType  string   `json:"device-type,omitempty"`
	DeviceRoles []string `json:"device-roles,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

//RoomInfo .
type RoomInfo struct {
	ID              string   `json:"id,omitempty"`
	DeploymentGroup string   `json:"deployment-group,omitempty"`
	Tags            []string `json:"tags,omitempty"`
}
