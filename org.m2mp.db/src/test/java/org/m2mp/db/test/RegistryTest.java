/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.registry.RegistryNode;
import org.m2mp.db.registry.file.DbFile;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

/**
 * @author Florent Clairambault
 */
public class RegistryTest {

    @BeforeClass
    public static void setUpClass() {
        DB.keyspace("ks_test", true);
        DbFile.prepareTable();
    }

    @Before
    public void setUp() {
        DB.execute("truncate RegistryNode;");
        DB.execute("truncate RegistryNodeChildren;");
        DB.execute("truncate RegistryNodeData;");
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

    @Test
    public void children() {
        new RegistryNode("/ch/a/bc").check();
        new RegistryNode("/ch/b/cd").check();
        new RegistryNode("/ch/c/de").check();

        { // We check that we find the 3 children
            RegistryNode parent = new RegistryNode("/ch");
            Assert.assertEquals(3, Lists.newLinkedList(parent.getChildren()).size());
        }

        new RegistryNode("/ch/a/bc2").check();

        { // We check that we find the 2 sub-children

            RegistryNode parent = new RegistryNode("/ch/a");
            Assert.assertEquals(2, Lists.newLinkedList(parent.getChildren()).size());
        }
    }

    @Test
    public void propertyTest() {
        { // We define
            RegistryNode node = new RegistryNode("/test/prop").check();
            node.setProperty("p1", "v1");
            node.setProperty("p2", "v2");
        }

        { // We test
            RegistryNode node = new RegistryNode("/test/prop").check();
            Assert.assertEquals("v1", node.getProperty("p1", null));
            Assert.assertEquals("v2", node.getProperty("p2", null));
        }

        { // We delete a property
            RegistryNode node = new RegistryNode("/test/prop").check();
            node.delProperty("p1");
        }

        { // We test
            RegistryNode node = new RegistryNode("/test/prop").check();
            Assert.assertEquals(null, node.getProperty("p1", null));
            Assert.assertEquals("v2", node.getProperty("p2", null));
        }

        { // We mark the node as deleted
            new RegistryNode("/test/prop").delete();
        }

        { // We test
            RegistryNode node = new RegistryNode("/test/prop").check();
            Assert.assertEquals(null, node.getProperty("p1", null));
            Assert.assertEquals("v2", node.getProperty("p2", null));
        }

        { // We delete the node for real
            new RegistryNode("/test/prop").delete(true);
        }

        { // We test
            RegistryNode node = new RegistryNode("/test/prop").check();
            Assert.assertEquals(null, node.getProperty("p1", null));
            Assert.assertEquals(null, node.getProperty("p2", null));
        }
    }

    @Test
    public void jsonFromNodeTest() {
        RegistryNode node = new RegistryNode("/test/json").check();
        Date a1 = new Date();
        UUID a2 = UUID.randomUUID();
        Date b1 = new Date();
        UUID b2 = UUID.randomUUID();

        {
            RegistryNode a = node.getChild("a").check();
            a.setProperty("a1", a1);
            a.setProperty("a2", a2);
            a.setProperty("a3", false);
        }
        {
            RegistryNode b = node.getChild("b").check();
            b.setProperty("b1", b1);
            b.setProperty("b2", b2);
            b.setProperty("b3", true);
        }
        node = new RegistryNode("/test/json");
        System.out.println(node.toJsonString()); // it's not what I expected
    }

    @Test
    public void copyTest() {
        RegistryNode root = new RegistryNode("/root/" + System.currentTimeMillis()).check();
        { // We define A
            RegistryNode a = root.getChild("a").check();
            a.setProperty("p0", "abc");
            a.setProperty("p1", "def");
            RegistryNode a1 = a.getChild("a1").check();
            a1.setProperty("p2", "ghi");
            a1.setProperty("p3", "klm");
        }
        { // We copy it to B
            root.getChild("a").copyTo(root.getChild("b"));
        }
        { // We check if B is like A
            RegistryNode b = root.getChild("b");
            Assert.assertEquals("abc", b.getPropertyString("p0"));
            Assert.assertEquals("def", b.getPropertyString("p1"));
            RegistryNode a1 = b.getChild("a1").check();
            Assert.assertEquals("ghi", a1.getPropertyString("p2"));
            Assert.assertEquals("klm", a1.getPropertyString("p3"));
        }
    }

    @Test
    public void moveTest() {
        RegistryNode root = new RegistryNode("/root/" + UUID.randomUUID()).check();
        { // We define A
            RegistryNode a = root.getChild("a").check();
            a.setProperty("p0", "abc");
            a.setProperty("p1", "def");
            RegistryNode a1 = a.getChild("a1").check();
            a1.setProperty("p2", "ghi");
            a1.setProperty("p3", "klm");
        }
        { // We copy it to B
            root.getChild("a").moveTo(root.getChild("b"), true);
        }
        { // We check if B is like A
            RegistryNode b = root.getChild("b");
            Assert.assertEquals("abc", b.getPropertyString("p0"));
            Assert.assertEquals("def", b.getPropertyString("p1"));
            RegistryNode a1 = b.getChild("a1").check();
            Assert.assertEquals("ghi", a1.getPropertyString("p2"));
            Assert.assertEquals("klm", a1.getPropertyString("p3"));
        }
        { // We check that A disappeared for real
            RegistryNode a = root.getChild("a");
            Assert.assertFalse(a.exists());

            RegistryNode a1 = root.getChild("a1");
            Assert.assertFalse(a1.exists());
        }
    }

    @Test
    public void dottedChildTests() {
        RegistryNode root = new RegistryNode("/root/" + UUID.randomUUID()).check();
        { // We define A
            RegistryNode a = root.getChild("a").create();
            a.getChild("a1").create();
        }
        { // We define B
            RegistryNode b = root.getChild("b").create();
            b.getChild("a2").create();
            b.getChild(".a3").create();
        }

        // Names check
        Assert.assertEquals(1, Lists.newArrayList(root.getChild("a").getChildrenNames()).size());
        Assert.assertEquals(2, Lists.newArrayList(root.getChild("b").getChildrenNames()).size());
        Assert.assertEquals(1, Lists.newArrayList(root.getChild("b").getChildrenUndottedNames()).size());

        // API correctness check
        Assert.assertEquals(1, Lists.newArrayList(root.getChild("a").getChildren()).size());
        Assert.assertEquals(2, Lists.newArrayList(root.getChild("b").getChildren()).size());
        Assert.assertEquals(2, Lists.newArrayList(root.getChild("b").getChildren(true)).size());
        Assert.assertEquals(1, Lists.newArrayList(root.getChild("b").getChildren(false)).size());
    }
}
