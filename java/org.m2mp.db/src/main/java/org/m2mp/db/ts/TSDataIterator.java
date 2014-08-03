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
     * Key of the serie
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
     * Current period: year * 12 + month
     */
    //private long period;
    //private final long
    /**
     * Beginning period
     */
    //        periodBegin,
    /**
     * Ending period
     */
    //periodEnd;
    /**
     * Inverted
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
        this.key = id + (type != null ? "!" + type : "");
        this.dateBegin = dateBegin != null ? dateBegin : UUIDs.startOf(System.currentTimeMillis() - (1000L * 3600 * 24 * 365 * 2)); // By default, 2 years back, at most 25 empty periods
        this.dateEnd = dateEnd != null ? dateEnd : UUIDs.endOf(System.currentTimeMillis() + (1000L * 3600 * 24 * 7)); // By default, 7 days ahead, at most 1 empty period
        //this.periodBegin = ;
        //this.periodEnd = ;
        this.inverted = inverted;
        this.periodIterator = new TSPeriodIterator(id, type, uuidToDate(this.dateBegin), uuidToDate(this.dateEnd), this.inverted);

        // We're either starting from the end or the beginning
        // inverted : from periodEnd to periodBegin
        // not inverted : from periodBegin to periodEnd
        // setPeriod(inverted ? periodEnd : periodBegin);
    }

    private static final Date uuidToDate(UUID u) {
        return u != null ? new Date(UUIDs.unixTimestamp(u)) : null;
    }

    private Iterator<Row> iter;

    final static FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("UTC"));

    private void setPeriod(String period) {
        //this.period = period;
        PreparedStatement ps = inverted ? reqSelectOrderDesc() : reqSelectOrderAsc();
        //String date = DATE_FORMAT.format(period);
        if (DEBUG)
            System.out.println("Searching in " + period + " from " + dateBegin + " to " + dateEnd);
        ResultSet rs = DB.execute(ps.bind(key, period, dateBegin, dateEnd));
        iter = rs.iterator();
    }

    private final long DAY_DURATION = 24 * 3600 * 1000;

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
        TimedData td = new TimedData(row.getString(0), row.getString(2), row.getUUID(1), row.getString(3));
        return td;
    }

    @Override
    public void remove() {
    }

    private static final String SELECT_COMMON = "SELECT id, time, type, data FROM " + TABLE_TIMESERIES + " WHERE id = ? AND date = ? AND time > ? AND time < ? ORDER BY time";

    public PreparedStatement reqSelectOrderAsc() {
        if (reqSelectOrderAsc == null) {
            reqSelectOrderAsc = DB.prepare(SELECT_COMMON + " ASC;");
        }
        return reqSelectOrderAsc;
    }

    private PreparedStatement reqSelectOrderAsc;

    public PreparedStatement reqSelectOrderDesc() {
        if (reqSelectOrderDesc == null) {
            reqSelectOrderDesc = DB.prepare(SELECT_COMMON + " DESC;");
        }
        return reqSelectOrderDesc;
    }

    private PreparedStatement reqSelectOrderDesc;
}