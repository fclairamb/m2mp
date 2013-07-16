package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DBAccess;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 */
public class TimeSerie {

//	static final String TABLE_TIMESERIES = "TimeSeries";
	public static void prepareTable(DBAccess db) {
		TableCreation.checkTable(db, new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE_TIMESERIES;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_TIMESERIES + " ( id text, period int, type text, date timeuuid, data text, PRIMARY KEY ((id, period), date) ) WITH CLUSTERING ORDER BY (date DESC);"));
				return list;
			}

			@Override
			public int getTableDefVersion() {
				return 1;
			}
		});
	}

	public static int dateToPeriod(UUID date) {
		return dateToPeriod(new Date(UUIDs.unixTimestamp(date)));
	}

	public static int dateToPeriod(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int period = cal.get(Calendar.YEAR) * 12 + (cal.get(Calendar.MONTH) + 1);
		return period;
	}

	public static void save(DBAccess db, TimedData td) {
		UUID dateUuid = td.getDateUUID();
		Date date = td.getDate();
		int period = dateToPeriod(date);
		PreparedStatement reqInsert = db.prepare("INSERT INTO " + TABLE_TIMESERIES + " ( id, period, type, date, data ) VALUES ( ?, ?, ?, ?, ? );");
		db.execute(reqInsert.bind(td.getId(), period, td.getType(), dateUuid, td.getJson()));
		if (td.getType() != null) {
			db.execute(reqInsert.bind(td.getId() + "!" + td.getType(), period, td.getType(), dateUuid, td.getJson()));
		}
	}

	public static Iterable<TimedData> getData(DBAccess db, String id, String type) {
		return getData(db, id, type, (Date) null, null, true);
	}

	public static Iterable<TimedData> getData(DBAccess db, String id, String type, Date dateBegin, Date dateEnd, boolean reverse) {
		return getData(db, id, type, dateBegin != null ? UUIDs.startOf(dateBegin.getTime()) : null, dateEnd != null ? UUIDs.endOf(dateEnd.getTime()) : null, reverse);
	}

	public static Iterable<TimedData> getData(DBAccess db, String id, String type, UUID dateBegin, UUID dateEnd, boolean reverse) {
		return new GetDataIterable(db, id, type, dateBegin, dateEnd, reverse);
	}
	public static final String TABLE_TIMESERIES = "TimeSeries";
}
