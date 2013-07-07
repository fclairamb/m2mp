package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
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

	public static Session sess() {
		return sessionWrapper.getSession();
	}

	public static KeyspaceMetadata meta() {
		return sessionWrapper.getKs();
	}
}
