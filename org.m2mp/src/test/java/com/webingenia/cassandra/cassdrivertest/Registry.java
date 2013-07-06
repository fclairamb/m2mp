/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.webingenia.cassandra.cassdrivertest;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.Shared;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author florent
 */
public class Registry {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			Shared.db().execute("drop table RegistryNode;");
			Shared.db().execute("drop table RegistryNodeChildren;");
			Shared.db().execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		RegistryNode.prepareTable();
	}

	@Test
	public void existence() {
		// We select "n1" and check that it doesn't exists
		RegistryNode n1 = new RegistryNode("/my/path/to/node/1");
		Assert.assertFalse(n1.exists());
		Assert.assertFalse(n1.existed());

		// We make sure it exists
		n1.check();
		Assert.assertTrue(n1.exists());
		RegistryNode parent = n1.getParentNode();
		Assert.assertTrue(n1.exists());
		Assert.assertEquals(parent.getPath(), "/my/path/to/node/");
		{
			ArrayList<RegistryNode> list = Lists.newArrayList(parent.getChildren());
			Assert.assertEquals(list.size(), 1);
		}

		// We delete it
		n1.delete();
		Assert.assertFalse(n1.exists());
		Assert.assertTrue(n1.existed());
		{
			ArrayList<RegistryNode> list = Lists.newArrayList(parent.getChildren());
			Assert.assertEquals(list.size(), 0);
		}

		// We recreate it
		n1.check();
		Assert.assertTrue(n1.exists());
		{
			ArrayList<RegistryNode> list = Lists.newArrayList(parent.getChildren());
			Assert.assertEquals(list.size(), 1);
		}

		Assert.assertEquals(5, new RegistryNode("/").getNbChildren(true));

		new RegistryNode("/my/path/to").delete();
		Assert.assertEquals(2, new RegistryNode("/").getNbChildren(true));
	}
}
