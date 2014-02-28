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

		Assert.assertEquals(d2, d3);
	}

    @Test(expected = IllegalArgumentException.class)
    public void test_rename_wrong() {
        Domain d1 = Domain.create("d3");
        Domain.create("d4");

        d1.setName("d4");
    }

    @Test
    public void test_rename_ok() {
        Domain d1 = Domain.create("d5");
        d1.setName("d6");

        Assert.assertEquals("d6", d1.getName());

        // They should have the same id
        Assert.assertEquals(Domain.get("d6"), d1);
    }
}
