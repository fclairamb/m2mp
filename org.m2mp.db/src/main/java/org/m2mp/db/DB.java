package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.m2mp.db.common.SessionWrapper;

/**
 *
 * @author Florent Clairambault
 */
public class DB {

	private final String name;
	private final SessionWrapper db;

	public DB(String name) {
		this.name = name;
		db = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name);
	}

	public Session session() {
		return db.getSession();
	}

	public KeyspaceMetadata meta() {
		return db.meta();
	}

	public PreparedStatement prepare(String query) {
		return session().prepare(query);
	}

	public ResultSet execute(Query q) {
		return session().execute(q);
	}

	public ResultSet execute(String q) {
		return session().execute(q);
	}
	public final DbFiles files = new DbFiles();

	public class DbFiles {

		public static final String TABLE_REGISTRYDATA = "RegistryNodeData";

		public PreparedStatement reqGetBlock() {
			if (reqGetBlock == null) {
				reqGetBlock = prepare("SELECT data FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
			}
			return reqGetBlock;
		}
		private PreparedStatement reqGetBlock;

		public PreparedStatement reqSetBlock() {
			if (reqSetBlock == null) {
				reqSetBlock = prepare("INSERT INTO " + TABLE_REGISTRYDATA + " ( path, block, data ) VALUES ( ?, ?, ? );");
			}
			return reqSetBlock;
		}
		private PreparedStatement reqSetBlock;

		public PreparedStatement reqDelBlock() {
			if (reqDelBlock == null) {
				reqDelBlock = prepare("DELETE FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
			}
			return reqDelBlock;
		}
		private PreparedStatement reqDelBlock;
	}
	public final User user = new User();

	public class User {

		public static final String TABLE_USER = "User";

		public PreparedStatement reqGetFromName() {
			if (reqGetIdFromName == null) {
				reqGetIdFromName = prepare("SELECT id FROM " + TABLE_USER + " WHERE name = ?;");
			}
			return reqGetIdFromName;
		}
		private PreparedStatement reqGetIdFromName;

		public PreparedStatement reqInsert() {
			if (reqInsert == null) {
				reqInsert = prepare("INSERT INTO " + TABLE_USER + " ( name, id, domain ) VALUES ( ?, ?, ? );");
			}
			return reqInsert;
		}
		private PreparedStatement reqInsert;
	}
	public final Domain domain = new Domain();

	public class Domain {

		public static final String TABLE_DOMAIN = "Domain";

		public PreparedStatement reqGetIdFromName() {
			if (reqGetIdFromName == null) {
				reqGetIdFromName = prepare("SELECT id FROM " + TABLE_DOMAIN + " WHERE name = ?;");
			}
			return reqGetIdFromName;
		}
		private PreparedStatement reqGetIdFromName;

		public PreparedStatement reqInsertDomain() {
			if (reqInsertDomain == null) {
				reqInsertDomain = prepare("INSERT INTO " + TABLE_DOMAIN + " ( name, id ) VALUES ( ?, ? );");
			}
			return reqInsertDomain;
		}
		private PreparedStatement reqInsertDomain;
	}
	public final GeneralSetting generalSetting = new GeneralSetting();

	public class GeneralSetting {

		public static final String TABLE_GENERAL_SETTINGS = "GeneralSetting";

		public PreparedStatement reqGet() {
			if (reqGet == null) {
				reqGet = prepare("SELECT value FROM " + TABLE_GENERAL_SETTINGS + " WHERE name = ?;");
			}
			return reqGet;
		}
		private PreparedStatement reqGet;

		public PreparedStatement reqSet() {
			if (reqSet == null) {
				reqSet = prepare("INSERT INTO " + TABLE_GENERAL_SETTINGS + " ( name, value ) VALUES ( ?, ? );");
			}
			return reqSet;
		}
		private PreparedStatement reqSet;
	}
	public final TimeSerie timeSerie = new TimeSerie();

	public class TimeSerie {

		public static final String TABLE_TIMESERIES = "TimeSeries";
		private static final String SELECT_COMMON = "SELECT id, type, date, data FROM " + TABLE_TIMESERIES + " WHERE id = ? AND period = ? AND date > ? AND date < ? ORDER BY date";

		public PreparedStatement reqSelectOrderAsc() {
			if (reqSelectOrderAsc == null) {
				reqSelectOrderAsc = prepare(SELECT_COMMON + " ASC;");
			}
			return reqSelectOrderAsc;
		}
		private PreparedStatement reqSelectOrderAsc;

		public PreparedStatement reqSelectOrderDesc() {
			if (reqSelectOrderDesc == null) {
				reqSelectOrderDesc = prepare(SELECT_COMMON + " DESC;");
			}
			return reqSelectOrderDesc;
		}
		private PreparedStatement reqSelectOrderDesc;

		public PreparedStatement reqInsert() {
			if (reqInsert == null) {
				reqInsert = prepare("INSERT INTO " + TABLE_TIMESERIES + " ( id, period, type, date, data ) VALUES ( ?, ?, ?, ?, ? );");
			}
			return reqInsert;
		}
		private PreparedStatement reqInsert;
	}
}
