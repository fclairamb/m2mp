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
 * Minimalistic cassandra access wrapper.
 * @author Florent Clairambault
 */
public class DB {

	private DB() {
	}
	private static String keyspaceName;
	private static Session session;

	/**
	 * Change keyspace
	 * @param name Name of the keyspace
	 * @param create To create it if not already there
	 */
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

	/**
	 * Change keyspace
	 * @param name Keyspace name
	 */
	public static void keyspace(String name) {
		keyspace(name, false);
	}

	/**
	 * Get the internal session object
	 * @return Session object
	 */
	public static Session session() {
		return session;
	}

	/**
	 * Get the keyspace metadata
	 * @return Metadata object
	 */
	public static KeyspaceMetadata meta() {
		return session.getCluster().getMetadata().getKeyspace(keyspaceName);
	}
	private static final LoadingCache<String, PreparedStatement> psCache = CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader<String, PreparedStatement>() {
		@Override
		public PreparedStatement load(String query) throws Exception {
			return prepareNoCache(query);
		}
	});

	/**
	 * Prepare a query and put it in cache.
	 * @param query Query to prepare
	 * @return PreparedStatement
	 */
	public static PreparedStatement prepare(String query) {
		try {
			return psCache.get(query);
		} catch (ExecutionException ex) {
			Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
			return session().prepare(query);
		}
	}

	/**
	 * Prepare a query (whithout putting it in cache)
	 * @param query Query to prepare
	 * @return PreparedStatement
	 */
	public static PreparedStatement prepareNoCache(String query) {
		return session.prepare(query);
	}

	/**
	 * Execute a query
	 * @param query Query to execute
	 * @return The result
	 */
	public static ResultSet execute(Query query) {
		return session.execute(query);
	}

	/**
	 * Execute a query
	 * @param query Query to execute
	 * @return The result
	 */
	public static ResultSet execute(String query) {
		return session.execute(query);
	}

	/**
	 * Execute a query asynchronously
	 * @param query Query to execute
	 * @return The result future
	 */
	public static ResultSetFuture executeAsync(Query query) {
		return session.executeAsync(query);
	}
}