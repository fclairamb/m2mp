/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.webingenia.cassandra.cassdrivertest;

import java.util.UUID;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.Domain;
import org.m2mp.db.Shared;
import org.m2mp.db.common.TableCreation;

/**
 *
 * @author florent
 */
public class Domains {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			Shared.db().execute("drop table Domain;");
			Shared.db().execute("drop table RegistryNode;");
			Shared.db().execute("drop table RegistryNodeChildren;");
			Shared.db().execute("drop table RegistryNodeData;");
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
		UUID id = d2.getId();

		Domain d3 = Domain.get("d1");

		Assert.assertEquals(d2.getId(), d3.getId());
	}
}
