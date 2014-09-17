package org.m2mp.db.common;

import com.datastax.driver.core.TableMetadata;
import org.m2mp.db.DB;

/**
 * Table creation helper.
 * <p/>
 * This is a very simple.
 *
 * @author Florent Clairambault
 */
public class TableCreation {


    public static void checkTable(TableIncrementalDefinition tableDef) {
        int version;

        if (!tableExists(tableDef.getTableDefName())) {
            version = -1;
        } else {
            version = GeneralSetting.get("table_version_" + tableDef.getTableDefName(), 0);
        }
        try {
            for (TableIncrementalDefinition.TableChange tc : tableDef.getTableDefChanges()) {
                if (tc.version > version) {
                    System.out.println("Executing \"" + tc.cql + "\"...");
                    try {
                        DB.execute(tc.cql);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    version = tc.version;
                }
            }
        } finally {
            GeneralSetting.set("table_version_" + tableDef.getTableDefName(), version);
        }
    }

    private static boolean tableExists(String tableName) {
        TableMetadata table = DB.meta().getTable(tableName.toLowerCase());
        return table != null;
    }
}
