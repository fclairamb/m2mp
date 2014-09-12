package org.m2mp.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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

    private static Cluster getCluster() {
        if (cluster == null) {
            cluster = Cluster.builder().addContactPoint("localhost").build();
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
     * @param name   Name of the keyspace
     * @param create To create it if not already there
     */
    public static void keyspace(String name, boolean create) {
        try {
            keyspaceName = name;
            psCache.cleanUp();
            session = getCluster().connect(keyspaceName);
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
        return session;
    }

    /**
     * Get the keyspace metadata
     *
     * @return Metadata object
     */
    public static KeyspaceMetadata meta() {
        return session.getCluster().getMetadata().getKeyspace(keyspaceName);
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
        return session.prepare(query);
    }

    /**
     * Execute a query
     *
     * @param query Query to execute
     * @return The result
     */
    public static ResultSet execute(Statement query) {
        return session.execute(query);
    }

    /**
     * Execute a query
     *
     * @param query Query to execute
     * @return The result
     */
    public static ResultSet execute(String query) {
        return session.execute(query);
    }

    /**
     * Execute a query asynchronously
     *
     * @param query Query to execute
     * @return The result future
     */
    public static ResultSetFuture executeAsync(Statement query) {
        return session.executeAsync(query);
    }

    /**
     * Execute a query later.
     * We can't really say when.
     *
     * @param query Query to execute
     */
    public static void executeLater(final Statement query) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    session.executeAsync(query);
                } catch (Exception e) {
                }
            }
        });
    }
}
