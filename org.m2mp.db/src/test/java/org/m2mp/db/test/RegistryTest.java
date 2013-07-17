/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DBAccess;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author Florent Clairambault
 */
public class RegistryTest {

	private static DBAccess db;

	@BeforeClass
	public static void setUpClass() {
		db = DBAccess.getOrCreate("ks_test");
		try {
			db.execute("drop table RegistryNode;");
			db.execute("drop table RegistryNodeChildren;");
			db.execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		RegistryNode.prepareTable(db);
	}

	@Test
	public void existence() {
		// We select "n1" and check that it doesn't exists
		RegistryNode n1 = new RegistryNode(db, "/my/path/to/node/1");
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

		Assert.assertEquals(5, new RegistryNode(db, "/").getNbChildren(true));

		new RegistryNode(db, "/my/path/to").delete();
		Assert.assertEquals(2, new RegistryNode(db, "/").getNbChildren(true));
	}

	@Test
	public void values() {
		RegistryNode node = new RegistryNode(db, "/my/path").check();
		Assert.assertNull(node.getProperty("myprop", null));
		node.setProperty("prop", "abc");

		node = new RegistryNode(db, "/my/path");
		Assert.assertEquals("abc", node.getProperty("prop", "___"));
	}

	@Test
	public void children() {
		new RegistryNode(db, "/ch/a/bc").check();
		new RegistryNode(db, "/ch/b/cd").check();
		new RegistryNode(db, "/ch/c/de").check();

		{ // We check that we find the 3 children
			RegistryNode parent = new RegistryNode(db, "/ch");
			Assert.assertEquals(3, Lists.newLinkedList(parent.getChildren()).size());
		}

		new RegistryNode(db, "/ch/a/bc2").check();

		{ // We check that we find the 2 sub-children

			RegistryNode parent = new RegistryNode(db, "/ch/a");
			Assert.assertEquals(2, Lists.newLinkedList(parent.getChildren()).size());
		}
	}

	@Test
	public void propertyTest() {
		{ // We define
			RegistryNode node = new RegistryNode(db, "/test/prop").check();
			node.setProperty("p1", "v1");
			node.setProperty("p2", "v2");
		}

		{ // We test
			RegistryNode node = new RegistryNode(db, "/test/prop").check();
			Assert.assertEquals("v1", node.getProperty("p1", null));
			Assert.assertEquals("v2", node.getProperty("p2", null));
		}

		{ // We delete a property
			RegistryNode node = new RegistryNode(db, "/test/prop").check();
			node.delProperty("p1");
		}

		{ // We test
			RegistryNode node = new RegistryNode(db, "/test/prop").check();
			Assert.assertEquals(null, node.getProperty("p1", null));
			Assert.assertEquals("v2", node.getProperty("p2", null));
		}

		{ // We mark the node as deleted
			new RegistryNode(db, "/test/prop").delete();
		}

		{ // We test
			RegistryNode node = new RegistryNode(db, "/test/prop").check();
			Assert.assertEquals(null, node.getProperty("p1", null));
			Assert.assertEquals("v2", node.getProperty("p2", null));
		}

		{ // We delete the node for real
			new RegistryNode(db, "/test/prop").delete(true);
		}

		{ // We test
			RegistryNode node = new RegistryNode(db, "/test/prop").check();
			Assert.assertEquals(null, node.getProperty("p1", null));
			Assert.assertEquals(null, node.getProperty("p2", null));
		}
	}
}
