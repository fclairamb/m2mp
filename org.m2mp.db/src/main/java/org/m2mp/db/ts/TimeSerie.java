package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;
import java.text.SimpleDateFormat;
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

	private static Calendar periodToCalendar(int period) {
		int year = period / 12;
		int month = period - (year * 12);
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month - 1);
		return cal;
	}

	public static String periodToMonth(int period) {
		return new SimpleDateFormat("yyyy-MM").format(periodToCalendar(period).getTime());
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
	 * Delete all data around a period.
	 *
	 * @param period Period to delete
	 * @param id Identifier of the data
	 * @param type Type of the data
	 */
	public static void delete(int period, String id, String type) {
		DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND period=?;").bind(id, period));
		if (type != null) {
			DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND period=?;").bind(id + "!" + type, period));
		}
	}

	/**
	 * Delete a precise event.
	 *
	 * @param date Date
	 * @param id Identifier
	 * @param type Type of the data
	 */
	public static void delete(String id, String type, UUID date) {
		int period = dateToPeriod(date);
		DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND period=? AND date=?;").bind(id, period, date));
		if (type != null) {
			DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND period=? AND date=?;").bind(id + "!" + type, period, date));
		}
	}

	/**
	 * Delete a precise event.
	 *
	 * @param td Timed data to delete
	 */
	public static void delete(TimedData td) {
		delete(td.getId(), td.getType(), td.getDateUUID());
	}

	/**
	 * Delete events from a date to a date with a period (month) precision.
	 *
	 * This only has a monthly precision. An other (uneeded at this time) proper
	 * cleanup is required to only delete the matching events in the fromPeriod
	 * and toPeriod periods.
	 *
	 * Specifying the type if very imporant here, without it the type won't be
	 * handled properly.
	 *
	 * @param id TS identifier
	 * @param type TS event type
	 * @param fromDate Beginning date
	 * @param toDate Ending date
	 */
	public static void deleteRoughly(String id, String type, Date fromDate, Date toDate) {
		if (fromDate == null) { // No start == 10 years back
			fromDate = new Date(System.currentTimeMillis() - 10 * 365 * 24 * 3600 * 1000);
		}
		if (toDate == null) { // No stop == 1 year after now
			toDate = new Date(System.currentTimeMillis() + 365 * 24 * 3600 * 1000);
		}
		int fromPeriod = dateToPeriod(fromDate), toPeriod = dateToPeriod(toDate);

		for (int period = fromPeriod; period <= toPeriod; period++) {
			delete(period, id, type);
		}
	}

	/**
	 * Delete events from a date to a date with correct (ms) precision.
	 *
	 * The type will be used for the deletion even if not issued. This is
	 * because every event is fetched to be deleted.
	 *
	 * @param id TS identifier
	 * @param type TS event type
	 * @param fromDate Beginning date
	 * @param toDate Ending date
	 */
	public static void delete(String id, String type, Date fromDate, Date toDate) {
		for (TimedData td : getData(id, type, fromDate, toDate, true)) {
			delete(td);
		}
	}

	/**
	 * Delete everything on a timeserie.
	 *
	 * This delete events 10 years before and 1 year after now.
	 *
	 * If the type is null, it won't be deleted. It's very important to delete
	 * the type as well.
	 *
	 * @param id TS Identifier
	 * @param type TS Type
	 */
	public static void delete(String id, String type) {
		deleteRoughly(id, type, null, null);
	}

	/**
	 * Get the latest data around an identifier.
	 *
	 * @param id Identifier
	 * @return TimedData iterator
	 */
	public static Iterable<TimedData> getData(String id) {
		return getData(id, null);
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
	 * Get some data around an identifier and a specific period (a month in the
	 * current implementation)
	 *
	 * @param id Identifier
	 * @param type Type of data
	 * @param period Period considered
	 * @param reverse Order of listing
	 * @return
	 */
	public static Iterable<TimedData> getData(String id, String type, int period, boolean reverse) {
		Calendar cal = periodToCalendar(period);
		Date begin, end;
		{
			cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
			cal.set(Calendar.HOUR, cal.getActualMinimum(Calendar.HOUR));
			cal.set(Calendar.MINUTE, cal.getActualMinimum(Calendar.MINUTE));
			cal.set(Calendar.SECOND, cal.getActualMinimum(Calendar.SECOND));
			cal.set(Calendar.MILLISECOND, cal.getActualMinimum(Calendar.MILLISECOND));
			begin = cal.getTime();
		}
		{
			cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
			cal.set(Calendar.HOUR, cal.getActualMaximum(Calendar.HOUR));
			cal.set(Calendar.MINUTE, cal.getActualMaximum(Calendar.MINUTE));
			cal.set(Calendar.SECOND, cal.getActualMaximum(Calendar.SECOND));
			cal.set(Calendar.MILLISECOND, cal.getActualMaximum(Calendar.MILLISECOND));
			end = cal.getTime();
		}
		return getData(id, type, begin, end, reverse);
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
