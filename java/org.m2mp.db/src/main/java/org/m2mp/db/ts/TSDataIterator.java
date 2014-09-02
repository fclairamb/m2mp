package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.commons.lang.time.FastDateFormat;
import org.m2mp.db.DB;

import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.UUID;

import static org.m2mp.db.ts.TimeSerie.TABLE_TIMESERIES;

/**
 * Time serie timed data iterator.
 *
 * @author Florent Clairambault
 */
public class TSDataIterator implements Iterator<TimedData> {

    private static final boolean DEBUG = false;

    /**
     * Id of the element of the serie.
     */
    private final String id;

    /**
     * Type of the serie.
     */
    private final String type;
    /**
     * Key of the serie. Will be in the form of <code>id</code> or
     * <code>id+"!"+type</code>
     */
    private final String key;
    private final UUID /**
     * Beginning date
     */
            dateBegin,
    /**
     * Ending date
     */
    dateEnd;
    /**
     * If the data is listed in the inverted order. true is DESC
     */
    private final boolean inverted;

    private final TSPeriodIterator periodIterator;

    /**
     * Data iterator.
     *
     * @param id        Identifier of the data to iterate on.
     * @param type      Type of data (can be null)
     * @param dateBegin Beginning date
     * @param dateEnd   Ending date
     * @param inverted  Inversion: true to go from end to beginning
     */
    TSDataIterator(String id, String type, UUID dateBegin, UUID dateEnd, boolean inverted) {
        this.id = id;
        this.type = type;
        this.key = id + (type != null ? "!" + type : "");
        this.dateBegin = dateBegin != null ? dateBegin : UUIDs.startOf(System.currentTimeMillis() - (1000L * 3600 * 24 * 365 * 2)); // By default, 2 years back, at most 25 empty periods
        this.dateEnd = dateEnd != null ? dateEnd : UUIDs.endOf(System.currentTimeMillis() + (1000L * 3600 * 24 * 7)); // By default, 7 days ahead, at most 1 empty period
        this.inverted = inverted;
        this.periodIterator = new TSPeriodIterator(id, type, uuidToDate(this.dateBegin), uuidToDate(this.dateEnd), this.inverted);
    }

    private static Date uuidToDate(UUID u) {
        return u != null ? new Date(UUIDs.unixTimestamp(u)) : null;
    }

    /**
     * Data iterator.
     * <p/>
     * The data iterator is used to list all the rows of the current period.
     * When the period is over, we will switch.
     */
    private Iterator<Row> iter;

    final static FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("UTC"));

    /**
     * Set the period.
     *
     * @param period Period to use
     *               <p/>
     *               In the current implementation, the period is the day in the form of
     *               yyyy-MM-dd.
     *               <p/>
     *               This method defines the iterator that we will use until the current
     *               period is over.
     */
    private void setPeriod(String period) {
        PreparedStatement ps = inverted ? reqSelectOrderDesc() : reqSelectOrderAsc();
        if (DEBUG) {
            System.out.println("Searching in " + period + " from " + dateBegin + " to " + dateEnd);
        }
        ResultSet rs = DB.execute(ps.bind(key, period, dateBegin, dateEnd));
        iter = rs.iterator();
    }

    @Override
    /**
     * Check if there's some data available.
     *
     * If we can't find some data, we will switch to the next period until we've
     * reached the last period.
     *
     */
    public boolean hasNext() {
        boolean hasNext = iter != null && iter.hasNext();
        while (!hasNext && periodIterator.hasNext()) {
            setPeriod(periodIterator.next());
            hasNext = iter.hasNext();
        }
        return hasNext;
    }

    @Override
    public TimedData next() {
        Row row = iter.next();
        String rowType = row.getString(0);
        if (rowType == null) {
            rowType = this.type;
        }
        TimedData td = new TimedData(id, rowType, row.getUUID(1), row.getString(2));
        return td;
    }

    @Override
    public void remove() {
    }

    private static final String SELECT_COMMON = "SELECT type, time, data FROM " + TABLE_TIMESERIES + " WHERE id = ? AND date = ? AND time > ? AND time < ? ORDER BY time";

    /**
     * Prepare a statement in the ASC order.
     *
     * @return Prepared statement
     */
    private PreparedStatement reqSelectOrderAsc() {
        if (reqSelectOrderAsc == null) {
            reqSelectOrderAsc = DB.prepare(SELECT_COMMON + " ASC;");
        }
        return reqSelectOrderAsc;
    }

    private PreparedStatement reqSelectOrderAsc;

    /**
     * Prepare a statement in the DESC order.
     *
     * @return Prepared statement
     */
    private PreparedStatement reqSelectOrderDesc() {
        if (reqSelectOrderDesc == null) {
            reqSelectOrderDesc = DB.prepare(SELECT_COMMON + " DESC;");
        }
        return reqSelectOrderDesc;
    }

    private PreparedStatement reqSelectOrderDesc;
}
