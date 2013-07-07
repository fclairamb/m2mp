/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.UUID;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.entity.Domain;
import org.m2mp.db.DB;

/**
 *
 * @author Florent Clairambault
 */
public class Domains {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			DB.session().execute("drop table Domain;");
			DB.session().execute("drop table RegistryNode;");
			DB.session().execute("drop table RegistryNodeChildren;");
			DB.session().execute("drop table RegistryNodeData;");
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
