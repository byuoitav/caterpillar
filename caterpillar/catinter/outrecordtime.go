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
	StartTime        time.Time `json:"start-time"`
	EndTime          time.Time `json:"end-time"`
	ElapsedInSeconds int64     `json:"elapsed-in-seconds"`
	RecordType       string    `json:"record-type"`

	Device DeviceInfo `json:"device"`
	Room   RoomInfo   `json:"room"`

	Input     string `json:"input"`
	InputType string `json:"input-type"`

	Volume  int    `json:"volume"`
	Blanked bool   `json:"blanked"`
	Muted   bool   `json:"muted"`
	Power   string `json:"power"`
}

//DeviceInfo .
type DeviceInfo struct {
	ID          string   `json:"id"`
	DeviceType  string   `json:"device-type"`
	DeviceRoles []string `json:"device-roles"`
	Tags        []string `json:"tags"`
}

//RoomInfo .
type RoomInfo struct {
	ID              string   `json:"id"`
	DeploymentGroup string   `json:"deployment-group"`
	Tags            []string `json:"tags"`
}
