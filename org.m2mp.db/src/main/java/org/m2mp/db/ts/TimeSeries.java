package org.m2mp.db.ts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DB;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 */
public class TimeSeries {

	static final String TABLE_TIMESERIES = "TimeSeries";

	public static void prepareTable() {
		TableCreation.checkTable(new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE_TIMESERIES;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_TIMESERIES + " ( id text, period int, type text, date timestamp, data text, PRIMARY KEY ((id, period), date) ) WITH CLUSTERING ORDER BY (date DESC);"));
				return list;
			}

			@Override
			public int getTableDefVersion() {
				return 1;
			}
		});
	}

	private static PreparedStatement reqInsert() {
		if (reqInsert == null) {
			reqInsert = DB.session().prepare("INSERT INTO " + TABLE_TIMESERIES + " ( id, period, type, date, data ) VALUES ( ?, ?, ?, ?, ? );");
		}
		return reqInsert;
	}
	private static PreparedStatement reqInsert;

	public static int dateToPeriod(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int period = cal.get(Calendar.YEAR) * 12 + (cal.get(Calendar.MONTH) + 1);
		return period;
	}

	public static void save(TimedData td) {
		Date date = td.getDate();
		int period = dateToPeriod(date);
		Session db = DB.session();
		db.execute(reqInsert().bind(td.getId(), period, td.getType(), date, td.getJson()));
		if (td.getType() != null) {
			db.execute(reqInsert().bind(td.getId() + "!" + td.getType(), period, td.getType(), date, td.getJson()));
		}
	}

	public static Iterable<TimedData> getData(String id, String type, Date dateBegin, Date dateEnd, boolean reverse) {
		return new GetDataIterable(id, type, dateBegin, dateEnd, reverse);
	}
}
