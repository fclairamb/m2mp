/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.entity.Domain;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class DomainTest {

//	private static DB db;
	@BeforeClass
	public static void setUpClass() {
		DB.keyspace("ks_test", true);
		try {
			DB.execute("drop table Domain;");
			DB.execute("drop table RegistryNode;");
			DB.execute("drop table RegistryNodeChildren;");
			DB.execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		Domain.prepareTable();
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_wrong_access() {
		new Domain("d1-wa");
	}

	@Test
	public void test_create() {
		Domain d1 = Domain.get("d1");
		Assert.assertNull(d1);

		Domain d2 = Domain.create("d1");

		Domain d3 = Domain.get("d1");

		Assert.assertEquals(d2.getId(), d3.getId());
	}
}
