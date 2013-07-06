package org.m2mp.db;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 */
public class TimeSeries {

	private static final String TABLE_TIMESERIES = "TimeSeries";

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
	private static PreparedStatement _reqInsert;

	private static PreparedStatement reqInsert() {
		if (_reqInsert == null) {
			_reqInsert = Shared.db().prepare("INSERT INTO " + TABLE_TIMESERIES + " ( id, period, type, date, data ) VALUES ( ?, ?, ?, ?, ? );");
		}
		return _reqInsert;
	}

	public static void save(TimedData td) {
		Date date = td.getDate();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int period = cal.get(Calendar.YEAR) * 12 + (cal.get(Calendar.MONTH) + 1);
		Session db = Shared.db();
		db.execute(reqInsert().bind(td.getId(), period, td.getType(), date, td.getJson()));
		db.execute(reqInsert().bind(td.getId() + "!" + td.getType(), period, td.getType(), date, td.getJson()));
	}
}
