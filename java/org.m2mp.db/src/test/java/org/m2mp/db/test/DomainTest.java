/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.entity.Domain;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author Florent Clairambault
 */
public class DomainTest {

//	private static DB db;
	@BeforeClass
	public static void setUpClass() {
		DB.keyspace("ks_test", true);
        RegistryNode.dropTable();
        RegistryNode.prepareTable();
    }

    @Test
    public void test_wrong_access() {
        Domain d1 = Domain.byName("d1-wa", false);
        Assert.assertNull(d1);
    }

	@Test
	public void test_create() {
        Domain d1 = Domain.byName("d1", false);
        Assert.assertNull(d1);

        Domain d2 = Domain.byName("d1", true);

        Domain d3 = Domain.byName("d1", false);

		Assert.assertEquals(d2, d3);
	}

    @Test(expected = IllegalArgumentException.class)
    public void test_rename_wrong() {
        Domain d1 = Domain.byName("d3", true);
        Domain.byName("d4", true);

        d1.setName("d4");
    }

    @Test
    public void test_rename_ok() {
        Domain d1 = Domain.byName("d5", true);
        d1.setName("d6");

        Assert.assertEquals("d6", d1.getName());

        // They should have the same id
        Assert.assertEquals(Domain.byName("d6", false), d1);
    }
}
