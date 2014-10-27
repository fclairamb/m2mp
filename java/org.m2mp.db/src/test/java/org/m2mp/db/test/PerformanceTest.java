/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.datastax.driver.core.PreparedStatement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.common.GeneralSetting;

/**
 * @author Florent Clairambault
 */
public class PerformanceTest {

    //	static DB db;
    @BeforeClass
    public static void setUpClass() {
        DB.setMode(DB.Mode.LocalOnly);
        DB.keyspace("ks_test", true);
        GeneralSetting.prepareTable();
    }

    private static final int NB_TESTS = 10000;

    @Test
    public void shouldBeSlow() { // 21.5s
        long before = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            DB.execute(DB.prepareNoCache("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );").bind("name", "value"));
        }
        long spent = System.currentTimeMillis() - before;
        System.out.println("SLOW: Time per test = " + ((double) spent / NB_TESTS) + " ms");
    }

    @Test
    public void shouldBeFast() { // 8.9s
        PreparedStatement ps = DB.prepareNoCache("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );");
        long before = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            DB.execute(ps.bind("name", "value"));
        }
        long spent = System.currentTimeMillis() - before;
        System.out.println("FAST: Time per test = " + ((double) spent / NB_TESTS) + " ms");
    }

    @Test
    public void iWonderAboutThisOne() { // 8.9s
        long before = System.currentTimeMillis();
        for (int i = 0; i < NB_TESTS; i++) {
            DB.execute(DB.prepare("INSERT INTO " + GeneralSetting.TABLE + " ( name, value ) VALUES ( ?, ? );").bind("name", "value"));
        }
        long spent = System.currentTimeMillis() - before;
        System.out.println("IWONDER: Time per test = " + ((double) spent / NB_TESTS) + " ms");
    }
}
