package org.m2mp.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.*;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimalistic cassandra access wrapper.
 *
 * @author Florent Clairambault
 */
public class DB {

    private static final PoolingOptions poolingOptions = new PoolingOptions() {
        {
            setCoreConnectionsPerHost(HostDistance.LOCAL, 1);
            setCoreConnectionsPerHost(HostDistance.REMOTE, 1);
            setMaxConnectionsPerHost(HostDistance.LOCAL, 50);
            setMaxConnectionsPerHost(HostDistance.REMOTE, 50);
        }
    };
    private static final SocketOptions socketOptions = new SocketOptions() {
        {
            setConnectTimeoutMillis(2000);
        }
    };
    private static final LoadingCache<String, PreparedStatement> psCache = CacheBuilder.newBuilder().maximumSize(100).build(new CacheLoader<String, PreparedStatement>() {
        @Override
        public PreparedStatement load(String query) throws Exception {
            return prepareNoCache(query).setConsistencyLevel(level);
        }
    });
    private static Executor executor = Executors.newCachedThreadPool();
    private static String keyspaceName;
    private static Cluster cluster;
    private static Session session;
    private static List<String> contactPoints = new ArrayList<String>() {
        {
            add("localhost");
        }
    };

    private DB() {
    }

    public enum Mode {
        Standard,
        Nearest,
        LocalOnly
    }

    private static Mode mode = Mode.Nearest;

    public static void setMode(Mode m) {
        mode = m;
        reset();
    }

    private static long slowQueryThreshold = 1000;

    public static void setSlowQueryThreshold(long threshold) {
        slowQueryThreshold = threshold;
    }

    public static Mode getMode() {
        return mode;
    }

    private static ConsistencyLevel level = ConsistencyLevel.ONE;

    public static void setConsistencyLevel(ConsistencyLevel level) {
        DB.level = level;
        reset();
    }

    public static ConsistencyLevel getConsistencyLevel() {
        return DB.level;
    }

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
        latencyPolicy = null;
        psCache.cleanUp();
    }

    private static LoadBalancingPolicy latencyPolicy;

    public static LoadBalancingPolicy getLoadBalancingPolicy() {
        if (latencyPolicy == null) {
            switch (mode) {
                case LocalOnly: {
                    ArrayList<InetSocketAddress> list = new ArrayList<InetSocketAddress>() {
                        {
                            add(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042));
                        }
                    };
                    latencyPolicy = new WhiteListPolicy(new RoundRobinPolicy(), list);
                    break;
                }
                case Nearest: {
                    latencyPolicy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).withRetryPeriod(15, TimeUnit.MINUTES).withScale(5, TimeUnit.SECONDS).withExclusionThreshold(1.5).build();
                    break;
                }
                default:
            }
        }
        return latencyPolicy;
    }

    private static Cluster getCluster() {
        if (cluster == null) {
            Cluster.Builder c = Cluster.builder();
            for (String cp : contactPoints) {
                c.addContactPoint(cp);
            }

            c.withPoolingOptions(poolingOptions)
                    .withSocketOptions(socketOptions)
                    .withReconnectionPolicy(new ExponentialReconnectionPolicy(10000, 900000));

            { // We apply a specific load balancing policy if we have one
                LoadBalancingPolicy policy = getLoadBalancingPolicy();
                if (policy != null) {
                    c.withLoadBalancingPolicy(policy);
                }
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
     * @param name   Name of the keyspace
     * @param create To create it if not already there
     *               <p/>
     *               Create option should never be set to free. It is only used for testing.
     */
    public static void keyspace(String name, boolean create) {
        keyspaceName = name;
        reset();
        if (create)
            try {
                session();
            } catch (RuntimeException ex) {
                if (create && ex.getCause() instanceof InvalidQueryException) {
                    String cql = "CREATE KEYSPACE " + name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};";
                    System.out.println("Executing: " + cql);
                    getCluster().connect().execute(cql);
                } else {
                    throw new RuntimeException("Could not load keyspace " + name + " !", ex.getCause());
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
                Cluster c = getCluster();
                Metadata metadata = c.getMetadata();


                Logger.getLogger(DB.class.getName()).log(Level.INFO, String.format("Connecting to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

                session = c.connect(keyspaceName);

                Logger.getLogger(DB.class.getName()).log(Level.INFO, String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));
            } catch (Exception ex) {
                cluster = null;
                throw new RuntimeException("Could not connect to the cluster", ex);
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
        return session().prepare(query).setConsistencyLevel(level);
    }

    /**
     * Execute a query
     *
     * @param query Query to execute
     * @return The result
     */
    public static ResultSet execute(Statement query) {
        long before = System.currentTimeMillis();
        ResultSet rs = session().execute(query);
        long timeSpent = System.currentTimeMillis() - before;
        if (timeSpent > slowQueryThreshold && query instanceof BoundStatement) {
            BoundStatement bs = (BoundStatement) query;
            PreparedStatement ps = bs.preparedStatement();

            try {
                ExecutionInfo execInfo = rs.getExecutionInfo();
                System.out.println("SLOW QUERY: [" + timeSpent + " / " + execInfo.getQueriedHost() + "] " + ps.getQueryString());
                QueryTrace queryTrace = execInfo.getQueryTrace();
                if (queryTrace != null) {
                    List<QueryTrace.Event> events = queryTrace.getEvents();
                    if (events != null) {
                        for (QueryTrace.Event ev : events) {
                            System.out.println("  * " + ev.getDescription() + ", " + ev.getSourceElapsedMicros());
                        }
                    }
                }
            } catch (Exception ex) {
                System.err.println("Ex: " + ex);
            }
        }
        return rs;
    }

    /**
     * Execute a query
     *
     * @param query Query to execute
     * @return The result
     */
    public static ResultSet execute(String query) {
        BoundStatement statement = prepare(query).bind();
        statement.setConsistencyLevel(level);
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
