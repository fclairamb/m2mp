package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DB;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 * Time series management.
 *
 * @author Florent Clairambault
 */
public class TimeSerie {

	/**
	 * Prepare the time serie table
	 */
	public static void prepareTable() {
		TableCreation.checkTable(new TableIncrementalDefinition() {
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

	/**
	 * Convert a date to a period
	 *
	 * @param date Date
	 * @return period
	 */
	static int dateToPeriod(UUID date) {
		return dateToPeriod(new Date(UUIDs.unixTimestamp(date)));
	}

	/**
	 * Convert a date to a period
	 *
	 * @param date Date
	 * @return period
	 */
	public static int dateToPeriod(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int period = cal.get(Calendar.YEAR) * 12 + (cal.get(Calendar.MONTH) + 1);
		return period;
	}

	/**
	 * Saved a timed data
	 *
	 * @param td Timed data to save
	 */
	public static void save(TimedData td) {
		UUID date = td.getDateUUID();
		int period = dateToPeriod(date);
		PreparedStatement reqInsert = DB.prepare("INSERT INTO " + TABLE_TIMESERIES + " ( id, period, type, date, data ) VALUES ( ?, ?, ?, ?, ? );");

		// We insert it once
		DB.execute(reqInsert.bind(td.getId(), period, td.getType(), date, td.getData()));

		// And also an other time if a type was specified
		if (td.getType() != null) {
			DB.execute(reqInsert.bind(td.getId() + "!" + td.getType(), period, td.getType(), date, td.getData()));
		}
	}

	/**
	 * Get the latest data around an identifier.
	 *
	 * @param id Identifier
	 * @return TimedData iterator
	 */
	public static Iterable<TimedData> getData(String id) {
		return getData(id);
	}

	/**
	 * Get the latest data around an identifier.
	 *
	 * @param id Identifier
	 * @param type Type of data
	 * @return TimedData iterator
	 */
	public static Iterable<TimedData> getData(String id, String type) {
		return getData(id, type, (Date) null, null, true);
	}

	/**
	 * Get some data around an identfier.
	 *
	 * @param id Identifier of the data
	 * @param type Type of data
	 * @param dateBegin Beginning date
	 * @param dateEnd Ending date
	 * @param reverse Order of listing
	 * @return TimedData irable
	 */
	public static Iterable<TimedData> getData(String id, String type, Date dateBegin, Date dateEnd, boolean reverse) {
		return getData(id, type, dateBegin != null ? UUIDs.startOf(dateBegin.getTime()) : null, dateEnd != null ? UUIDs.endOf(dateEnd.getTime()) : null, reverse);
	}

	/**
	 * Get some data around an identfier.
	 *
	 * @param id Identifier of the data
	 * @param type Type of data
	 * @param dateBegin Beginning date
	 * @param dateEnd Ending date
	 * @param reverse Order of listing
	 * @return TimedData irable
	 */
	public static Iterable<TimedData> getData(String id, String type, UUID dateBegin, UUID dateEnd, boolean reverse) {
		return new TSDataIterable(id, type, dateBegin, dateEnd, reverse);
	}
	public static final String TABLE_TIMESERIES = "TimeSeries";
}
