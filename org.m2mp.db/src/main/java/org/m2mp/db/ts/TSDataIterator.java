package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import java.util.*;
import org.m2mp.db.DB;
import static org.m2mp.db.ts.TimeSerie.TABLE_TIMESERIES;

/**
 * Time serie timed data iterator.
 *
 * @author Florent Clairambault
 */
public class TSDataIterator implements Iterator<TimedData> {

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
	private int period;
	private final int /**
			 * Beginning period
			 */
			periodBegin,
			/**
			 * Ending period
			 */
			periodEnd;
	/**
	 * Inverted
	 */
	private final boolean inverted;

	/**
	 * Data iterator.
	 *
	 * @param id Identifier of the data to iterate on.
	 * @param type Type of data (can be null)
	 * @param dateBegin Beginning date
	 * @param dateEnd Ending date
	 * @param inverted Inversion: true to go from end to beginning
	 */
	TSDataIterator(String id, String type, UUID dateBegin, UUID dateEnd, boolean inverted) {
		this.key = id + (type != null ? "!" + type : "");
		this.dateBegin = dateBegin != null ? dateBegin : UUIDs.startOf(System.currentTimeMillis() - (1000L * 3600 * 24 * 365 * 2)); // By default, 2 years back, at most 25 empty periods
		this.dateEnd = dateEnd != null ? dateEnd : UUIDs.endOf(System.currentTimeMillis() + (1000L * 3600 * 24 * 7)); // By default, 7 days ahead, at most 1 empty period
		this.periodBegin = TimeSerie.dateToPeriod(this.dateBegin);
		this.periodEnd = TimeSerie.dateToPeriod(this.dateEnd);
		this.inverted = inverted;

		// We're either starting from the end or the beginning
		// inverted : from periodEnd to periodBegin
		// not inverted : from periodBegin to periodEnd
		setPeriod(inverted ? periodEnd : periodBegin);
	}
	private Iterator<Row> iter;

	private void setPeriod(int period) {
		this.period = period;
		PreparedStatement ps = inverted ? reqSelectOrderDesc() : reqSelectOrderAsc();
		ResultSet rs = DB.execute(ps.bind(key, period, dateBegin, dateEnd));
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
		return new TimedData(row.getString(0), row.getString(1), row.getUUID(2), row.getString(3));
	}

	@Override
	public void remove() {
	}
	private static final String SELECT_COMMON = "SELECT id, type, date, data FROM " + TABLE_TIMESERIES + " WHERE id = ? AND period = ? AND date > ? AND date < ? ORDER BY date";

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