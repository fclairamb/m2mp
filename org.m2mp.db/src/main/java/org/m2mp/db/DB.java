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

	private static String name = "m2mp_v2";

	public static void setKeySpace(String name) {
		DB.name = name;
	}

	public static void switchToProduction() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name);
	}

	public static void switchToTest() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name + "_test");
	}
	public static SessionWrapper sessionWrapper;

	public static Session session() {
		return sessionWrapper.getSession();
	}

	public static KeyspaceMetadata meta() {
		return sessionWrapper.getKs();
	}

	public static PreparedStatement prepare(String query) {
		return session().prepare(query);
	}

	public static ResultSet execute(Query q) {
		return session().execute(q);
	}
}
