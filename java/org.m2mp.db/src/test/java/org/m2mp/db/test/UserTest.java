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
import org.m2mp.db.entity.User;
import org.m2mp.db.registry.RegistryNode;

/**
 * @author Florent Clairambault
 */
public class UserTest {

    @BeforeClass
    public static void setUpClass() {
        DB.keyspace("ks_test", true);
        RegistryNode.dropTable();
        RegistryNode.prepareTable();
    }

    @Test
    public void create() {
        {
            User user1 = User.byName("user1", false);
            Assert.assertNull(user1);
        }
        {
            User user1 = User.byName("name", true);
            User user2 = User.byName("name", false);
            Assert.assertNotNull(user1.getId());
            Assert.assertEquals(user1.getId(), user2.getId());
        }
    }

    @Test
    public void delete() {
        // Domain
        Domain d = Domain.byName("domain2", true);
        Assert.assertTrue(d.exists());
        Assert.assertTrue(d.getNode().exists());
        d.delete();
        Assert.assertFalse(d.exists());
    }
}
