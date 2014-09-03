package m2mpdb

import (
	"encoding/json"
	"github.com/gocql/gocql"
	"log"
	"time"
)

const DEBUG = false

const DATE_FORMAT = "2006-01-02"

func indexTSDate(i, d, t string) {
	// We need to make it better: create a go routine that will get it, index last stored value (to avoid doing it again), and store it
	shared.session.Query("insert into timeseries_index (id, date, type) values (?, ?, ?);", i, d, t).Exec()
}

func SaveTSUUID(id string, dataType string, utime *gocql.UUID, data string) error {
	date := utime.Time().Format(DATE_FORMAT)

	// We save the main data
	query := shared.session.Query("insert into timeseries (id, date, time, type, data) values (?, ?, ?, ?, ?);", id, date, *utime, dataType, data)
	if err := query.Exec(); err != nil {
		return err
	}
	indexTSDate(id, date, "")

	// We save with a type
	query = shared.session.Query("insert into timeseries (id, date, time, data) values (?, ?, ?, ?);", id+"!"+dataType, date, *utime, data)
	if err := query.Exec(); err != nil {
		return err
	}
	indexTSDate(id, date, dataType)

	return nil
}

func SaveTSTime(id string, dataType string, time time.Time, data string) error {
	utime := gocql.UUIDFromTime(time)
	return SaveTSUUID(id, dataType, &utime, data)
}

func SaveTSTimeObj(id string, dataType string, time time.Time, obj interface{}) error {
	if data, err := json.Marshal(obj); err == nil {
		return SaveTSTime(id, dataType, time, string(data))
	} else {
		return err
	}
}

type TimedData struct {
	Id    string
	Type  string
	UTime gocql.UUID
	Data  string
}

func (this *TimedData) Time() time.Time {
	return this.UTime.Time()
}

func NewTimedDataFromUUID(id, dataType string, time gocql.UUID, data string) *TimedData {
	return &TimedData{Id: id, Type: dataType, UTime: time, Data: data}
}

func NewTimedDataFromTime(id, dataType string, time time.Time, data string) *TimedData {
	return &TimedData{Id: id, Type: dataType, UTime: gocql.UUIDFromTime(time), Data: data}
}

func newPeriodDataIterator(id, dataType string, begin, end *time.Time, inverted bool) *gocql.Iter {
	cql := "select date from timeseries_index where id=? and type=?"
	args := make([]interface{}, 0, 4)
	args = append(args, id, dataType)
	if begin != nil {
		cql += " and date<=?"
		args = append(args, begin)
	}
	if end != nil {
		cql += " and end>=?"
		args = append(args, end)
	}
	if inverted {
		cql += " order by date desc"
	} else {
		cql += " order by date asc"
	}

	if DEBUG {
		log.Println("Query:", cql, args)
	}

	return shared.session.Query(cql, args...).Iter()
}

func newDataIterator(id, dataType, date string, begin, end *time.Time, inverted bool) *gocql.Iter {
	cql := "select id, type, time, data from timeseries where id=? and date=?"
	args := make([]interface{}, 0, 4)

	if dataType != "" {
		id += "!" + dataType
	}

	args = append(args, id)
	args = append(args, date)
	if begin != nil {
		cql += " and time<=?"
		args = append(args, gocql.UUIDFromTime(*begin))
	}
	if end != nil {
		cql += " and time>=?"
		args = append(args, gocql.UUIDFromTime(*end))
	}
	if inverted {
		cql += " order by time desc;"
	} else {
		cql += " order by time asc;"
	}

	if DEBUG {
		log.Println("Query:", cql, args)
	}

	return shared.session.Query(cql, args...).Iter()

	//if err := query.Exec(); err != nil {
	//	log.Fatal(err)
	//}
	//return query.Iter()
}

type TSDataIterator struct {
	id         string
	dataType   string
	begin      *time.Time
	end        *time.Time
	inverted   bool
	periodIter *gocql.Iter
	dataIter   *gocql.Iter
}

func NewTSDataIterator(id string, dataType string, begin, end *time.Time, inverted bool) *TSDataIterator {
	periodIter := newPeriodDataIterator(id, dataType, begin, end, inverted)

	this := &TSDataIterator{id: id, dataType: dataType, begin: begin, end: end, periodIter: periodIter, inverted: inverted}

	return this
}

func (this *TSDataIterator) Close() {
	if this.periodIter != nil {
		this.periodIter.Close()
		this.periodIter = nil
	}
	if this.dataIter != nil {
		this.dataIter.Close()
		this.dataIter = nil
	}
}

func (this *TSDataIterator) Scan(td *TimedData) bool {
	for {
		// We try to get a timed data
		if this.dataIter != nil && this.dataIter.Scan(&td.Id, &td.Type, &td.UTime, &td.Data) {
			// We have to copy the data type in case we chose it (it might be useless in most usages, we'll see about that later)
			if this.dataType != "" {
				td.Type = this.dataType
			}
			return true
		} else { // If we couldn't, we have to change the period
			var date string
			if this.periodIter.Scan(&date) {
				//log.Printf("Date: %s", date)
				this.dataIter = newDataIterator(this.id, this.dataType, date, this.begin, this.end, this.inverted)
			} else {
				return false
			}
		}
	}
}
