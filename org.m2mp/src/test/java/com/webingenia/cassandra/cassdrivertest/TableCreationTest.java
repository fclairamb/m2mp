/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.webingenia.cassandra.cassdrivertest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.common.TableCreation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author florent
 */
public class TableCreationTest {

	public Cluster cluster;
	public Session session;

	@Before
	public void setUp() {
		cluster = Cluster.builder().addContactPoint("localhost").build();
		session = cluster.connect("ks");
	}
	
	@After
	public void tearDown() {
		cluster.shutdown();
	}

	@Test
	public void generalSettingsCreation() {
		TableCreation.checkTable(session, GeneralSetting.DEFINITION);
	}


}
