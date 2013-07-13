package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Florent Clairambault
 */
public class DB {

	private final String keyspaceName;
	private final Session session;

	public DB(String name) {
		keyspaceName = name;
		session = Cluster.builder().addContactPoint("localhost").build().connect(keyspaceName);
	}

	public Session session() {
		return session;
	}

	public KeyspaceMetadata meta() {
		return session.getCluster().getMetadata().getKeyspace(keyspaceName);
	}
	
	private final LoadingCache<String, PreparedStatement> psCache = CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader<String, PreparedStatement>() {
		@Override
		public PreparedStatement load(String query) throws Exception {
			return prepareNoCache(query);
		}
	});

	public PreparedStatement prepare(String query) {
		try {
			return psCache.get(query);
		} catch (ExecutionException ex) {
			Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
			return session().prepare(query);
		}
	}

	public final PreparedStatement prepareNoCache(String query) {
		return session.prepare(query);
	}

	public ResultSet execute(Query q) {
		return session.execute(q);
	}

	public ResultSet execute(String q) {
		return session.execute(q);
	}
}
