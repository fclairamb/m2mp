package org.m2mp.db.ts;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.m2mp.db.DB;

import java.util.Date;
import java.util.Iterator;


public class TSPeriodIterator implements Iterator<String> {


    private static final boolean DEBUG = false;

    private final Iterator<Row> iter;

    public TSPeriodIterator(String id, String type, Date begin, Date end, boolean inverted) {
        if (type == null) {
            type = "";
        }

        String after = inverted ? " ORDER BY date DESC;" : " ORDER BY date ASC;";
        ResultSet rs;
        if (begin != null && end != null) {
            rs = DB.execute(
                    DB.prepare(SELECT_BE + after).bind(id, type, TimeSerie.DATE_FORMAT.format(begin), TimeSerie.DATE_FORMAT.format(end))
            );
        } else if (begin != null) {
            rs = DB.execute(
                    DB.prepare(SELECT_B + after).bind(id, type, TimeSerie.DATE_FORMAT.format(begin))
            );
        } else if (end != null) {
            rs = DB.execute(
                    DB.prepare(SELECT_E + after).bind(id, type, TimeSerie.DATE_FORMAT.format(end))
            );
        } else {
            rs = DB.execute(
                    DB.prepare(SELECT_COMMON + after).bind(id, type)
            );
        }
        iter = rs.iterator();
    }

    private static final String SELECT_COMMON = "SELECT date FROM " + TimeSerie.TABLE_TIMESERIES_INDEX + " WHERE id=? and type=?";
    private static final String SELECT_BE = SELECT_COMMON + " and date>=? and date<=?";
    private static final String SELECT_B = SELECT_COMMON + " and date>=?";
    private static final String SELECT_E = SELECT_COMMON + " and date<=?";


    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public String next() {
        String next = iter.next().getString(0);
        if (DEBUG) {
            System.out.println("Next period: " + next);
        }
        return next;
    }

    @Override
    public void remove() {

    }
}
