/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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

	static {
		switchToProduction();
	}

	public static void switchToProduction() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), "ks");
	}

	public static void switchToTest() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), "ks_test");
	}
	public static SessionWrapper sessionWrapper;

	public static Session db() {
		return sessionWrapper.getSession();
	}

	public static KeyspaceMetadata dbMgmt() {
		return sessionWrapper.getKs();
	}
}
