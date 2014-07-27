package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.commons.lang.time.FastDateFormat;
import org.m2mp.db.DB;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
        GeneralSetting.prepareTable();
        TableCreation.checkTable(new TableIncrementalDefinition() {
            @Override
            public String getTableDefName() {
                return TABLE_TIMESERIES;
            }

            @Override
            public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
                List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
                list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_TIMESERIES + " (\n" +
                        "  id text,\n" +
                        "  date text,\n" +
                        "  time timeuuid,\n" +
                        "  data text,\n" +
                        "  type text,\n" +
                        "  PRIMARY KEY ((id, date), time)\n" +
                        ") WITH CLUSTERING ORDER BY (time DESC);"));

                list.add(new TableIncrementalDefinition.TableChange(2, "CREATE TABLE " + TABLE_TIMESERIES_INDEX + " (\n" +
                        "  id text,\n" +
                        "  date text,\n" +
                        "  type text,\n" +
                        "  PRIMARY KEY ((id), date)\n" +
                        ") WITH CLUSTERING ORDER BY (date DESC);"));
                return list;
            }

            @Override
            public int getTableDefVersion() {
                return 2;
            }
        });
    }

    /**
     * Convert a date to a period
     *
     * @param date Date
     * @return period
     */
    static String dateToDate10(UUID date) {
        return DATE_FORMAT.format(UUIDs.unixTimestamp(date));
    }

    static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("UTC"));

    private static Calendar date10ToCalendar(String date) {
        try {
            Date d = new SimpleDateFormat("yyyy-MM-dd").parse(date);

            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

            cal.setTime(d);

            return cal;
        } catch (ParseException ex) {
            System.err.println(ex); // No logging in this class
        }
        return null;
    }

    /*
    public static String periodToMonth(int period) {
        return new SimpleDateFormat("yyyy-MM").format(date10ToCalendar(period).getTime());
    }
    */

    /**
     * Save a timed data
     *
     * @param td Timed data to save
     */
    public static void save(TimedData td) {
        save(td.getId(), td.getType(), td.getDateUUID(), td.getData());
    }

    /**
     * Save a timed data wrapper
     *
     * @param tdw data wrapper to save
     */
    public static void save(TimedDataWrapper tdw) {
        save(tdw.getId(), tdw.getType(), tdw.getDateUUID(), tdw.getJson());
    }

    public static void save(String id, String type, UUID date, String data) {
        String date10 = dateToDate10(date);
        PreparedStatement reqInsert = DB.prepare("INSERT INTO " + TABLE_TIMESERIES + " ( id, date, time, type, data ) VALUES ( ?, ?, ?, ?, ? );");

        // We insert it once
        DB.execute(reqInsert.bind(id, date10, date, type, data));

        // And also an other time if a type was specified
        if (type != null) {
            DB.execute(reqInsert.bind(id + "!" + type, date10, date, type, data));
        }

        // We don't really care if it could be executed or not, what matters is that we have at least one period per id + type saved
        DB.execute(DB.prepare("INSERT INTO " + TABLE_TIMESERIES_INDEX + " ( id, type, date ) VALUES( ?, ?, ? );").bind(id, type, date10));
    }

    /**
     * Delete all data around a period.
     *
     * @param date Period to delete
     * @param id     Identifier of the data
     * @param type   Type of the data
     */
    public static void delete(String date, String id, String type) {
        DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND date=?;").bind(id, date));
        if (type != null) {
            DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND date=?;").bind(id + "!" + type, date));
        }
    }

    /**
     * Delete a precise data.
     *
     * @param date Date
     * @param id   Identifier
     * @param type Type of the data
     */
    public static void delete(String id, String type, UUID date) {
        String date10 = dateToDate10(date);
        DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND date=? AND time=?;").bind(id, date10, date));
        if (type != null) {
            DB.execute(DB.prepare("DELETE FROM " + TABLE_TIMESERIES + " WHERE id=? AND date=? AND time=?;").bind(id + "!" + type, date10, date));
        }
    }

    /**
     * Delete a TimedData.
     *
     * @param td Timed data to delete
     */
    public static void delete(TimedData td) {
        delete(td.getId(), td.getType(), td.getDateUUID());
    }

    /**
     * Delete a TimedDataWrapper.
     *
     * @param tdw Timed data wrapper to delete
     */
    public static void delete(TimedDataWrapper tdw) {
        delete(tdw.getId(), tdw.getType(), tdw.getDateUUID());
    }

    /**
     * Delete events from a date to a date with a period (month) precision.
     * <p/>
     * This only has a monthly precision. An other (uneeded at this time) proper
     * cleanup is required to only delete the matching events in the fromPeriod
     * and toPeriod periods.
     * <p/>
     * Specifying the type if very imporant here, without it the type won't be
     * handled properly.
     *
     * @param id       TS identifier
     * @param type     TS event type
     * @param fromDate Beginning date
     * @param toDate   Ending date
     */
    public static void deleteRoughly(String id, String type, Date fromDate, Date toDate) {
        for (String period : new TSPeriodIterable(id, type, fromDate, toDate, true)) {
            delete(period, id, type);
        }
    }

    /**
     * Delete events from a date to a date with correct (ms) precision.
     * <p/>
     * The type will be used for the deletion even if not issued. This is
     * because every event is fetched to be deleted.
     *
     * @param id       TS identifier
     * @param type     TS event type
     * @param fromDate Beginning date
     * @param toDate   Ending date
     */
    public static void delete(String id, String type, Date fromDate, Date toDate) {
        for (TimedData td : getData(id, type, fromDate, toDate, true)) {
            delete(td);
        }
    }

    /**
     * Delete everything on a timeserie.
     * <p/>
     * This delete events 10 years before and 1 year after now.
     * <p/>
     * If the type is null, it won't be deleted. It's very important to delete
     * the type as well.
     *
     * @param id   TS Identifier
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
        return getData(id, (String) null);
    }

    /**
     * Get the latest data around an identifier.
     *
     * @param id   Identifier
     * @param type Type of data
     * @return TimedData iterator
     */
    public static Iterable<TimedData> getData(String id, String type) {
        return getData(id, type, (Date) null, null, true);
    }

    /**
     * Get some data around an identifier and a specific date
     *
     * @param id      Identifier
     * @param type    Type of data
     * @param date10  Period considered
     * @param reverse Order of listing
     * @return
     */
    public static Iterable<TimedData> getData(String id, String type, String date10, boolean reverse) {
        Calendar cal = date10ToCalendar(date10);
        Date begin, end;
        {
            //cal.set(Calendar.DAY_OF_MONTH, cal.getActualMinimum(Calendar.DAY_OF_MONTH));
            cal.set(Calendar.HOUR, cal.getActualMinimum(Calendar.HOUR));
            cal.set(Calendar.MINUTE, cal.getActualMinimum(Calendar.MINUTE));
            cal.set(Calendar.SECOND, cal.getActualMinimum(Calendar.SECOND));
            cal.set(Calendar.MILLISECOND, cal.getActualMinimum(Calendar.MILLISECOND));
            begin = cal.getTime();
        }
        {
            //cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
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
     * @param id        Identifier of the data
     * @param type      Type of data
     * @param dateBegin Beginning date
     * @param dateEnd   Ending date
     * @param reverse   Order of listing
     * @return TimedData irable
     */
    public static Iterable<TimedData> getData(String id, String type, Date dateBegin, Date dateEnd, boolean reverse) {
        return getData(id, type, dateBegin != null ? UUIDs.startOf(dateBegin.getTime()) : null, dateEnd != null ? UUIDs.endOf(dateEnd.getTime()) : null, reverse);
    }

    /**
     * Get some data around an identfier.
     *
     * @param id        Identifier of the data
     * @param type      Type of data
     * @param dateBegin Beginning date
     * @param dateEnd   Ending date
     * @param reverse   Order of listing
     * @return TimedData irable
     */
    public static Iterable<TimedData> getData(String id, String type, UUID dateBegin, UUID dateEnd, boolean reverse) {
        return new TSDataIterable(id, type, dateBegin, dateEnd, reverse);
    }

    /**
     * Get a precise data.
     *
     * @param id   Identifier
     * @param time Date
     * @return data or null if not found
     */
    public static TimedData getData(String id, UUID time) {
        ResultSet result = DB.execute(DB.prepare("SELECT id, type, date, data FROM " + TABLE_TIMESERIES + " WHERE id = ? AND date = ? AND time = ?;").bind(id, dateToDate10(time), time));
        for (Row row : result) {
            return new TimedData(row.getString(0), row.getString(1), row.getUUID(2), row.getString(3));
        }
        return null;
    }


    public static final String TABLE_TIMESERIES = "timeseries";
    public static final String TABLE_TIMESERIES_INDEX = "timeseries_index";


}
