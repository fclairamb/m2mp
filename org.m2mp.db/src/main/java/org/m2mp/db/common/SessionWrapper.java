package org.m2mp.db.common;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 *
 * @author Florent Clairambault
 */
public class SessionWrapper {

	private final Session session;
	private final String keyspaceName;

	public SessionWrapper(Session session, String keyspaceName) {
		this.session = session;
		this.keyspaceName = keyspaceName;
	}

	public SessionWrapper(Cluster cluster, String ks) {
		this.session = cluster.connect(ks);
		this.keyspaceName = ks;
	}

	public Session getSession() {
		return session;
	}

	public KeyspaceMetadata meta() {
		return session.getCluster().getMetadata().getKeyspace(keyspaceName);
	}
}
