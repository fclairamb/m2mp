package org.m2mp.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimalistic cassandra access wrapper.
 *
 * @author Florent Clairambault
 */
public class DB {

	private DB() {
	}

	private static Executor executor = Executors.newCachedThreadPool();

	private static String keyspaceName;
	private static Cluster cluster;
	private static Session session;

	private static List<String> contactPoints = new ArrayList<String>() {
		{
			add("localhost");
		}
	};

	/**
	 * Change the default servers.
	 *
	 * @param contactPoints
	 */
	public static void setContactPoints(List<String> contactPoints) {
		DB.contactPoints = contactPoints;
		reset();
	}

	private static void reset() {
		cluster = null;
		session = null;
		psCache.cleanUp();
	}

	private static Cluster getCluster() {
		if (cluster == null) {
			Cluster.Builder c = Cluster.builder();
			for (String cp : contactPoints) {
				c.addContactPoint(cp);
			}
			cluster = c.build();
		}
		return cluster;
	}

	public static void stop() {
		if (cluster != null) {
			cluster.close();
			cluster = null;
		}
		psCache.cleanUp();
	}

	/**
	 * Change keyspace
	 *
	 * @param name Name of the keyspace
	 * @param create To create it if not already there
	 *
	 * Create option should never be set to free. It is only used for testing.
	 */
	public static void keyspace(String name, boolean create) {
		try {
			keyspaceName = name;
			reset();
		} catch (InvalidQueryException ex) {
			if (create) {
				cluster.connect().execute("CREATE KEYSPACE " + name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
			} else {
				throw new RuntimeException("Could not load keyspace " + name + " !", ex);
			}
		}
	}

	/**
	 * Change keyspace
	 *
	 * @param name Keyspace name
	 */
	public static void keyspace(String name) {
		keyspace(name, false);
	}

	/**
	 * Get the internal session object
	 *
	 * @return Session object
	 */
	public static Session session() {
		if (session == null) {
			if (keyspaceName == null) {
				throw new RuntimeException("You need to define a keyspace !");
			}
            try {
                session = getCluster().connect(keyspaceName);
            }
            catch( IllegalStateException ex ) {
                cluster = null;
            }
		}
		return session;
	}

	/**
	 * Get the keyspace metadata
	 *
	 * @return Metadata object
	 */
	public static KeyspaceMetadata meta() {
		return session().getCluster().getMetadata().getKeyspace(keyspaceName);
	}

	private static final LoadingCache<String, PreparedStatement> psCache = CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader<String, PreparedStatement>() {
		@Override
		public PreparedStatement load(String query) throws Exception {
			return prepareNoCache(query).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		}
	});

	/**
	 * Prepare a query and put it in cache.
	 *
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
	 *
	 * @param query Query to prepare
	 * @return PreparedStatement
	 */
	public static PreparedStatement prepareNoCache(String query) {
		return session().prepare(query).setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
	}

	/**
	 * Execute a query
	 *
	 * @param query Query to execute
	 * @return The result
	 */
	public static ResultSet execute(Statement query) {
		return session().execute(query);
	}

	/**
	 * Execute a query
	 *
	 * @param query Query to execute
	 * @return The result
	 */
	public static ResultSet execute(String query) {
		BoundStatement statement = prepare(query).bind();
		return session().execute(statement);
	}

	/**
	 * Execute a query asynchronously
	 *
	 * @param query Query to execute
	 * @return The result future
	 */
	public static ResultSetFuture executeAsync(Statement query) {
		return session().executeAsync(query);
	}

	/**
	 * Execute a query later. We can't really say when.
	 *
	 * @param query Query to execute
	 */
	public static void executeLater(final Statement query) {
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					session().executeAsync(query);
				} catch (Exception e) {
				}
			}
		});
	}
}
