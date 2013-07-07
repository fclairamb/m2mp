package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class GetDataIterator implements Iterator<TimedData> {

	private final String key;
	private final Date dateBegin, dateEnd;
	private int period;
	private final int periodBegin, periodEnd;
	private final boolean inverted;

	public GetDataIterator(String id, String type, Date dateBegin, Date dateEnd, boolean inverted) {
		this.key = id + (type != null ? "!" + type : "");
		this.dateBegin = dateBegin != null ? dateBegin : new Date(System.currentTimeMillis() - (1000L * 3600 * 24 * 365 * 2)); // By default, 2 years back, at most 25 empty periods
		this.dateEnd = dateEnd != null ? dateEnd : new Date(System.currentTimeMillis() + (1000L * 3600 * 24 * 7)); // By default, 7 days ahead, at most 1 empty period
		this.periodBegin = TimeSeries.dateToPeriod(this.dateBegin);
		this.periodEnd = TimeSeries.dateToPeriod(this.dateEnd);
		this.inverted = inverted;

		// We're either starting from the end or the beginning
		// inverted : from periodEnd to periodBegin
		// not inverted : from periodBegin to periodEnd
		setPeriod(inverted ? periodEnd : periodBegin);
	}
	private Iterator<Row> iter;
	private static final String SELECT_COMMON = "SELECT id, type, date, data FROM " + TimeSeries.TABLE_TIMESERIES + " WHERE id = ? AND period = ? AND date > ? AND date < ? ORDER BY date";

	private PreparedStatement reqSelectOrderAsc() {
		if (reqSelectOrderAsc == null) {
			reqSelectOrderAsc = DB.sess().prepare(SELECT_COMMON + " ASC;");
		}
		return reqSelectOrderAsc;
	}
	private PreparedStatement reqSelectOrderAsc;

	private PreparedStatement reqSelectOrderDesc() {
		if (reqSelectOrderDesc == null) {
			reqSelectOrderDesc = DB.sess().prepare(SELECT_COMMON + " DESC;");
		}
		return reqSelectOrderDesc;
	}
	private PreparedStatement reqSelectOrderDesc;

	private void setPeriod(int period) {
		this.period = period;
		PreparedStatement ps = inverted ? reqSelectOrderDesc() : reqSelectOrderAsc();
		ResultSet rs = DB.sess().execute(ps.bind(key, period, dateBegin, dateEnd));
		iter = rs.iterator();
	}

	@Override
	public boolean hasNext() {
		boolean hasNext = iter.hasNext();
		while (!hasNext && (inverted ? period >= periodBegin : period <= periodEnd)) {
			period += inverted ? -1 : 1;
			setPeriod(period);
			hasNext = iter.hasNext();
		}
		return hasNext;
	}

	@Override
	public TimedData next() {
		Row row = iter.next();
		return new TimedData(row.getString(0), row.getString(1), row.getDate(2), row.getString(3));
	}

	@Override
	public void remove() {
	}
}