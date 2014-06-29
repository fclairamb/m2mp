package m2mpdb

import (
	"encoding/json"
	"github.com/gocql/gocql"
	"time"
)

func timeToPeriod(t time.Time) int {
	return t.Year() * 12 + int(t.Month())
}

func SaveTSTime(id string, dataType string, time time.Time, data string) error {
	period := timeToPeriod(time)

	u := gocql.UUIDFromTime(time)

	query := shared.session.Query("insert into timeseries (id, period, type, date, data) values (?, ?, ?, ?, ?); ", id, period, dataType, u, data)
	if err := query.Exec(); err != nil {
		return err
	}

	query = shared.session.Query("insert into timeseries (id, period, date, data) values (?, ?, ?, ?); ", id+"!"+dataType, period, u, data)
	if err := query.Exec(); err != nil {
		return err
	}

	// The goal is to do it in an unreliable way. It could be done through a channel. That way at most one command is executed at the same time.
	go shared.session.Query("insert into timeseries_index (id, period, type) values (?, ?, ?);", id, period, dataType).Exec()

	return nil
}

func SaveTSTimeObj(id string, dataType string, time time.Time, obj interface{}) error {
	if data, err := json.Marshal(obj); err == nil {
		return SaveTSTime(id, dataType, time, string(data))
	} else {
		return err
	}
}
