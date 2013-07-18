package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
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

	private DB() {
	}
	private static String keyspaceName;
	private static Session session;

	public static void keyspace(String name, boolean create) {
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		try {
			keyspaceName = name;
			psCache.cleanUp();
			session = cluster.connect(keyspaceName);
		} catch (InvalidQueryException ex) {
			if (create) {
				cluster.connect().execute("CREATE KEYSPACE " + name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
			} else {
				keyspace(name, false);
			}
		}
	}

	public static void keyspace(String name) {
		keyspace(name, false);
	}

	public static Session session() {
		return session;
	}

	public static KeyspaceMetadata meta() {
		return session.getCluster().getMetadata().getKeyspace(keyspaceName);
	}
	private static final LoadingCache<String, PreparedStatement> psCache = CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader<String, PreparedStatement>() {
		@Override
		public PreparedStatement load(String query) throws Exception {
			return prepareNoCache(query);
		}
	});

	public static PreparedStatement prepare(String query) {
		try {
			return psCache.get(query);
		} catch (ExecutionException ex) {
			Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
			return session().prepare(query);
		}
	}

	public static PreparedStatement prepareNoCache(String query) {
		return session.prepare(query);
	}

	public static ResultSet execute(Query q) {
		return session.execute(q);
	}

	public static ResultSet execute(String q) {
		return session.execute(q);
	}

	public static ResultSetFuture executeAsync(Query q) {
		return session.executeAsync(q);
	}
}
