package org.m2mp.db.common;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 *
 * @author florent
 */
public class SessionWrapper {
	public final Session session;
	public final String keyspaceName;

	public SessionWrapper(Session session, String keyspaceName) {
		this.session = session;
		this.keyspaceName = keyspaceName;
	}

	public SessionWrapper(Cluster cluster, String ks) {
		this.session = cluster.connect(ks);
		this.keyspaceName = ks;
	}
}
