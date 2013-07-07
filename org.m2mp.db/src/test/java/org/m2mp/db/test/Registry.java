/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author Florent Clairambault
 */
public class Registry {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			DB.sess().execute("drop table RegistryNode;");
			DB.sess().execute("drop table RegistryNodeChildren;");
			DB.sess().execute("drop table RegistryNodeData;");
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

	@Test
	public void values() {
		RegistryNode node = new RegistryNode("/my/path").check();
		Assert.assertNull(node.getProperty("myprop", null));
		node.setProperty("prop", "abc");

		node = new RegistryNode("/my/path");
		Assert.assertEquals("abc", node.getProperty("prop", "___"));
	}
}
