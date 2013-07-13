package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import java.util.*;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class GetDataIterator implements Iterator<TimedData> {

	private final DB db;
	private final String key;
	private final UUID dateBegin, dateEnd;
	private int period;
	private final int periodBegin, periodEnd;
	private final boolean inverted;

	public GetDataIterator(DB db, String id, String type, UUID dateBegin, UUID dateEnd, boolean inverted) {
		this.db = db;
		this.key = id + (type != null ? "!" + type : "");
		this.dateBegin = dateBegin != null ? dateBegin : UUIDs.startOf(System.currentTimeMillis() - (1000L * 3600 * 24 * 365 * 2)); // By default, 2 years back, at most 25 empty periods
		this.dateEnd = dateEnd != null ? dateEnd : UUIDs.endOf(System.currentTimeMillis() + (1000L * 3600 * 24 * 7)); // By default, 7 days ahead, at most 1 empty period
		this.periodBegin = TimeSeries.dateToPeriod(this.dateBegin);
		this.periodEnd = TimeSeries.dateToPeriod(this.dateEnd);
		this.inverted = inverted;

		// We're either starting from the end or the beginning
		// inverted : from periodEnd to periodBegin
		// not inverted : from periodBegin to periodEnd
		setPeriod(inverted ? periodEnd : periodBegin);
	}
	private Iterator<Row> iter;

	private void setPeriod(int period) {
		this.period = period;
		PreparedStatement ps = inverted ? db.timeSerie.reqSelectOrderDesc() : db.timeSerie.reqSelectOrderAsc();
		ResultSet rs = db.execute(ps.bind(key, period, dateBegin, dateEnd));
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
}