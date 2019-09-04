package caterpillarcommon

import (
	"strings"
	"time"

	"github.com/byuoitav/caterpillar/v2/caterpillarmssql"
	"github.com/byuoitav/common/log"
	"github.com/byuoitav/wso2services/classschedules/calendar"
	"github.com/byuoitav/wso2services/classschedules/registrar"
)

var (
	exceptionDateList          []exceptionDateRecord
	exceptionDateMapExpiration time.Time
)

type exceptionDateRecord struct {
	ExceptionDateID int       `db:"ExceptionDateID"`
	ExceptionDate   time.Time `db:"ExceptionDate"`
	ExceptionType   string    `db:"ExceptionType"`
}

type classScheduleRecord struct {
	RoomScheduleID  int     `db:"RoomScheduleID"`
	RoomID          string  `db:"RoomID"`
	SchedulingType  string  `db:"SchedulingType"`
	RoomDescription string  `db:"RoomDescription"`
	RoomCapacity    int     `db:"RoomCapacity"`
	YearTerm        string  `db:"YearTerm"`
	DeptName        string  `db:"DeptName"`
	CatalogNumber   string  `db:"CatalogNumber"`
	CatalogSuffix   string  `db:"CatalogSuffix"`
	Honors          string  `db:"Honors"`
	ServLearning    string  `db:"ServLearning"`
	CreditHours     float64 `db:"CreditHours"`
	SectionType     string  `db:"SectionType"`
	ClassTime       string  `db:"ClassTime"`
	Days            string  `db:"Days"`
	InstructorName  string  `db:"InstructorName"`
	SectionSize     int     `db:"SectionSize"`
	TotalEnrolled   int     `db:"TotalEnrolled"`
	ScheduleType    string  `db:"ScheduleType"`
	AssignTo        string  `db:"AssignTo"`
}

type classDayRecord struct {
	ClassDayID    int       `db:"ClassDayID"`
	YearTerm      string    `db:"YearTerm"`
	RoomID        string    `db:"RoomID"`
	DeptName      string    `db:"DeptName"`
	CatalogNumber string    `db:"CatalogNumber"`
	StartDateTime time.Time `db:"StartDateTime"`
	EndDateTime   time.Time `db:"EndDateTime"`
}

//SyncClassScheduleWithDatabase ...
func SyncClassScheduleWithDatabase() {
	//go get the list of exception dates from the database
	getExceptionDateList()

	//go get a list of all the control dates
	yearTermDateList, nerr := calendar.GetControlDates()

	if nerr != nil {
		log.L.Errorf("Unable to get control dates %v", nerr.Error())
	}

	for _, year := range yearTermDateList {
		if time.Time(year.StartDate).Year() >= 2017 {
			//for any that are after 2017, get a list of buildings
			buildingList, err := registrar.GetBuildingsWithClassesForYearTerm(year.YearTerm)
			if err != nil {
				log.L.Errorf("Unable to get building list for %v: %v", year.YearTerm, err.Error())
			}

			for _, building := range buildingList {
				//then get a list of rooms
				roomList, err := registrar.GetRoomWithClassesForYearTermBuilding(year.YearTerm, building)
				if err != nil {
					log.L.Errorf("Unable to get room list for %v-%v: %v", year.YearTerm, building, err.Error())
				}

				for _, room := range roomList {

					roomParts := strings.Split(room, "-")
					//then get the schedule for each room
					roomSchedule, err := registrar.GetClassSchedulesForYearTermBuildingRoom(year.YearTerm, building, roomParts[1])
					if err != nil {
						log.L.Errorf("Unable to get room list for %v-%v: %v", year.YearTerm, building, err.Error())
					}

					//sync / compare / contrast
					//get the list from the db
					dbRecords, err := getClassScheduleListFromDB(year.YearTerm, room)
					if err != nil {
						log.L.Errorf("unable to get existing records from db: %v", err.Error())
					}

					//compare and delete / update if needed
					for _, classSchedule := range roomSchedule.Schedules {
						roomID := roomSchedule.Building + "-" + roomSchedule.Room
						key := year.YearTerm + "~" + roomID + "~" +
							classSchedule.DeptName + classSchedule.CatalogNumber + classSchedule.CatalogSuffix + "~" +
							classSchedule.ClassTime + classSchedule.Days

						dbRecord, ok := dbRecords[key]

						if !ok {
							//new
							log.L.Debugf("Adding %v %v", year.YearTerm, roomID)							
							err = addClassScheduleToDB(year.YearTerm, roomID, roomSchedule, classSchedule)

							if err != nil {
								log.L.Errorf("unable to get add class schedule to db: %v", err.Error())
							}
						} else {
							//compare
							if dbRecord.SchedulingType != roomSchedule.SchedulingType ||
								dbRecord.RoomDescription != roomSchedule.RoomDesc ||
								dbRecord.RoomCapacity != roomSchedule.Capacity ||
								dbRecord.Honors != classSchedule.Honors ||
								dbRecord.ServLearning != classSchedule.ServLearning ||
								dbRecord.CreditHours != classSchedule.CreditHours ||
								strings.TrimSpace(dbRecord.SectionType) != strings.TrimSpace(classSchedule.SectionType) ||
								dbRecord.InstructorName != classSchedule.InstructorName ||
								dbRecord.SectionSize != classSchedule.SectionSize ||
								dbRecord.TotalEnrolled != classSchedule.TotalEnr ||
								dbRecord.ScheduleType != classSchedule.SchedType ||
								dbRecord.AssignTo != classSchedule.AssignTo {
								//update
								log.L.Debugf("Updating %v %v", year.YearTerm, roomID)

								err = updateClassScheduleInDB(dbRecord, roomSchedule, classSchedule)

								if err != nil {
									log.L.Errorf("unable to update class schedule to db: %v", err.Error())
								}

							} else {
								log.L.Debugf("No need to change %v %v", year.YearTerm, roomID)
							}

							//remove it from the list so we know what to nuke at the end
							delete(dbRecords, key)
						}
					}

					if len(dbRecords) > 0 {
						//remove any stragglers
						for _, dbRecord := range dbRecords {
							log.L.Debugf("deleting rogue %v %v", year.YearTerm, dbRecord.RoomID)
							deleteClassScheduleInDB(dbRecord)
						}
					}

				}
			}
		}

		//now go through the database and sync up the ClassDays table with the ClassroomSchedule table for this term
	}
}

func getExceptionDateList() ([]exceptionDateRecord, error) {
	if len(exceptionDateList) == 0 || time.Now().Sub(exceptionDateMapExpiration).Hours() < 0 {
		//go get it from the DB
		db, err := caterpillarmssql.GetDB()

		if err != nil {
			log.L.Errorf("Unable to get DB: %v", err.Error())
			return exceptionDateList, err
		}

		q := `
		SELECT ExceptionDate, ExceptionType
		FROM ExceptionDates
		ORDER BY ExceptionDate
		`

		err = db.Select(&exceptionDateList, q)

		if err != nil {
			log.L.Errorf("Unable to get class schedule list from DB: %v", err.Error())
			return nil, err
		}

		exceptionDateMapExpiration = time.Now().Add(24 * time.Hour)
	}

	return exceptionDateList, nil
}

func getClassScheduleListFromDB(yearTerm, roomID string) (map[string]classScheduleRecord, error) {
	db, err := caterpillarmssql.GetDB()

	if err != nil {
		log.L.Errorf("Unable to get DB: %v", err.Error())
		return nil, err
	}

	q := `
		SELECT *
		FROM ClassroomSchedules		
		WHERE YearTerm = @p1
		AND RoomID = @p2
		`

	var response []classScheduleRecord
	err = db.Select(&response, q, yearTerm, roomID)

	if err != nil {
		log.L.Errorf("Unable to get class schedule list from DB: %v", err.Error())
		return nil, err
	}

	retValue := make(map[string]classScheduleRecord)

	for _, item := range response {
		retValue[item.YearTerm+"~"+item.RoomID+"~"+item.DeptName+item.CatalogNumber+item.CatalogSuffix+"~"+item.ClassTime+item.Days] = item
	}

	return retValue, nil
}

func addClassScheduleToDB(yearTerm, roomID string, roomSchedule registrar.Room, classSchedule registrar.ClassSchedule) error {
	db, err := caterpillarmssql.GetDB()

	if err != nil {
		log.L.Errorf("Unable to get DB: %v", err.Error())
		return err
	}

	_, err = db.NamedExec(`
								INSERT INTO ClassroomSchedules
								VALUES
								(
									:RoomID,
									:SchedulingType,
									:RoomDescription,
									:RoomCapacity,
									:YearTerm,
									:DeptName,
									:CatalogNumber,
									:CatalogSuffix,
									:Honors,
									:ServLearning,
									:CreditHours,
									:SectionType,
									:ClassTime,
									:Days,
									:InstructorName,
									:SectionSize,
									:TotalEnrolled,
									:ScheduleType,
									:AssignTo
								)`,
		map[string]interface{}{
			"RoomID":          strings.TrimSpace(roomID),
			"SchedulingType":  strings.TrimSpace(roomSchedule.SchedulingType),
			"RoomDescription": strings.TrimSpace(roomSchedule.RoomDesc),
			"RoomCapacity":    roomSchedule.Capacity,
			"YearTerm":        strings.TrimSpace(yearTerm),
			"DeptName":        strings.TrimSpace(classSchedule.DeptName),
			"CatalogNumber":   strings.TrimSpace(classSchedule.CatalogNumber),
			"CatalogSuffix":   strings.TrimSpace(classSchedule.CatalogSuffix),
			"Honors":          strings.TrimSpace(classSchedule.Honors),
			"ServLearning":    strings.TrimSpace(classSchedule.ServLearning),
			"CreditHours":     classSchedule.CreditHours,
			"SectionType":     strings.TrimSpace(classSchedule.SectionType),
			"ClassTime":       strings.TrimSpace(classSchedule.ClassTime),
			"Days":            strings.TrimSpace(classSchedule.Days),
			"InstructorName":  strings.TrimSpace(classSchedule.InstructorName),
			"SectionSize":     classSchedule.SectionSize,
			"TotalEnrolled":   classSchedule.TotalEnr,
			"ScheduleType":    strings.TrimSpace(classSchedule.SchedType),
			"AssignTo":        strings.TrimSpace(classSchedule.AssignTo),
		})

	if err != nil {
		log.L.Errorf("Unable to add class schedule record: %v", err.Error())
		return err
	}

	return nil
}

func updateClassScheduleInDB(dbRecord classScheduleRecord, roomSchedule registrar.Room, classSchedule registrar.ClassSchedule) error {
	db, err := caterpillarmssql.GetDB()

	if err != nil {
		log.L.Errorf("Unable to get DB: %v", err.Error())
		return err
	}

	_, err = db.NamedExec(`
							UPDATE ClassroomSchedules SET								
								SchedulingType = :SchedulingType, 
								RoomDescription = :RoomDescription, 
								RoomCapacity = :RoomCapacity, 								
								DeptName = :DeptName, 
								CatalogNumber = :CatalogNumber, 
								CatalogSuffix = :CatalogSuffix, 
								Honors = :Honors, 
								ServLearning = :ServLearning, 
								CreditHours = :CreditHours, 
								SectionType = :SectionType, 
								ClassTime = :ClassTime, 
								Days = :Days, 
								InstructorName = :InstructorName, 
								SectionSize = :SectionSize, 
								TotalEnrolled = :TotalEnrolled, 
								ScheduleType = :ScheduleType, 
								AssignTo = :AssignTo
							WHERE RoomScheduleID = :RoomScheduleID
								`,
		map[string]interface{}{
			"RoomScheduleID":  dbRecord.RoomScheduleID,
			"SchedulingType":  roomSchedule.SchedulingType,
			"RoomDescription": roomSchedule.RoomDesc,
			"RoomCapacity":    roomSchedule.Capacity,
			"DeptName":        classSchedule.DeptName,
			"CatalogNumber":   classSchedule.CatalogNumber,
			"CatalogSuffix":   classSchedule.CatalogSuffix,
			"Honors":          classSchedule.Honors,
			"ServLearning":    classSchedule.ServLearning,
			"CreditHours":     classSchedule.CreditHours,
			"SectionType":     classSchedule.SectionType,
			"ClassTime":       classSchedule.ClassTime,
			"Days":            classSchedule.Days,
			"InstructorName":  classSchedule.InstructorName,
			"SectionSize":     classSchedule.SectionSize,
			"TotalEnrolled":   classSchedule.TotalEnr,
			"ScheduleType":    classSchedule.SchedType,
			"AssignTo":        classSchedule.AssignTo,
		})

	if err != nil {
		log.L.Errorf("Unable to update class schedule record: %v", err.Error())
		return err
	}

	return nil
}

func deleteClassScheduleInDB(dbRecord classScheduleRecord) error {
	db, err := caterpillarmssql.GetDB()

	if err != nil {
		log.L.Errorf("Unable to get DB: %v", err.Error())
		return err
	}

	_, err = db.NamedExec(`
							DELETE FROM ClassroomSchedules 
							WHERE RoomScheduleID = :RoomScheduleID
								`,
		map[string]interface{}{
			"RoomScheduleID": dbRecord.RoomScheduleID,
		})

	if err != nil {
		log.L.Errorf("Unable to update class schedule record: %v", err.Error())
		return err
	}

	return nil
}
