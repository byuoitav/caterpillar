package catinter

import "time"

//recordTypes
const (
	Input      = "input"
	Power      = "power"
	Blank      = "blank"
	Volume     = "volume"
	Mute       = "mute"
	PowerCount = "power-count"
)

//MetricsRecord .
type MetricsRecord struct {
	StartTime        time.Time `json:"start-time,omitempty"`
	EndTime          time.Time `json:"end-time,omitempty"`
	ElapsedInSeconds int64     `json:"elapsed-in-seconds,omitempty"`
	RecordType       string    `json:"record-type,omitempty"`

	Device   DeviceInfo   `json:"device,omitempty"`
	Room     RoomInfo     `json:"room,omitempty"`
	Building BuildingInfo `json:"building,omitempty"`
	Class    ClassInfo    `json:"class,omitempty"`

	Input     string `json:"input,omitempty"`
	InputType string `json:"input-type,omitempty"`

	Volume     *int   `json:"volume,omitempty"`
	Blanked    *bool  `json:"blanked,omitempty"`
	Muted      *bool  `json:"muted,omitempty"`
	Power      string `json:"power,omitempty"`
	PowerCount *int   `json:"power-count,omitempty"`

	Tags []string `json:"tags"`
}

//ClassInfo .
type ClassInfo struct {
	DeptName        string  `json:"department,omitempty"`
	CatalogNumber   string  `json:"catlog-number,omitempty"`
	ClassName       string  `json:"class-name,omitempty"`
	CreditHours     float64 `json:"credit-hours,omitempty"`
	ClassSize       int     `json:"class-size,omitempty"`
	ClassEnrollment int     `json:"class-enrollment,omitempty"`
	Instructor      string  `json:"insuructor,omitempty"`

	ClassStart time.Time `json:"class-start,omitempty"`
	ClassEnd   time.Time `json:"class-end,omitempty"`
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

//BuildingInfo .
type BuildingInfo struct {
	ID              string   `json:"id,omitempty"`
	DeploymentGroup string   `json:"deployment-group,omitempty"`
	Tags            []string `json:"tags,omitempty"`
}
