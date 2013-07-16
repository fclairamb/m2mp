/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.entity.Domain;
import org.m2mp.db.DBAccess;

/**
 *
 * @author Florent Clairambault
 */
public class DomainTest {

	private static DBAccess db;

	@BeforeClass
	public static void setUpClass() {
		db = DBAccess.getOrCreate("ks_test");
		try {
			db.execute("drop table Domain;");
			db.execute("drop table RegistryNode;");
			db.execute("drop table RegistryNodeChildren;");
			db.execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		Domain.prepareTable(db);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_wrong_access() {
		new Domain(db, "d1-wa");
	}

	@Test
	public void test_create() {
		Domain d1 = Domain.get(db, "d1");
		Assert.assertNull(d1);

		Domain d2 = Domain.create(db, "d1");

		Domain d3 = Domain.get(db, "d1");

		Assert.assertEquals(d2.getId(), d3.getId());
	}
}
