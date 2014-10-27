package org.m2mp.db.test;

import com.datastax.driver.core.ConsistencyLevel;
import org.m2mp.db.DB;

/**
 * Created by florent on 26/10/14.
 */
public class General {
    public static void setUpClass() {
        DB.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        DB.setMode(DB.Mode.LocalOnly);
        DB.keyspace("ks_test", true);
    }
}
