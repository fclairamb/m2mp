package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.m2mp.db.common.SessionWrapper;

/**
 *
 * @author florent
 */
public class Shared {

	private static final String name = "ks";

	static {
		switchToProduction();
	}

	public static void switchToProduction() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name);
	}

	public static void switchToTest() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name + "_test");
	}
	public static SessionWrapper sessionWrapper;

	public static Session db() {
		return sessionWrapper.getSession();
	}

	public static KeyspaceMetadata dbMgmt() {
		return sessionWrapper.getKs();
	}
}
