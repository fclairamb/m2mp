/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.UUID;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.entity.Domain;
import org.m2mp.db.entity.User;
import org.m2mp.db.ts.TimeSerie;

/**
 *
 * @author Florent Clairambault
 */
public class UserTest {

	@BeforeClass
	public static void setUpClass() {
		DB.keyspace("ks_test",true);
		try {
			DB.execute("drop table " + Domain.TABLE + ";");
			DB.execute("drop table " + User.TABLE + ";");
		} catch (Exception ex) {
		}
		User.prepareTable();
	}

	@Test
	public void create() {
		// Domain
		Domain domain = Domain.create("domain");

		{
			User user1 = User.get("user1");
			Assert.assertNull(user1);
		}
		{
			User user1 = User.create("name", domain);
			User user2 = User.get("name");
			Assert.assertNotNull(user1.getId());
			Assert.assertEquals(user1.getId(), user2.getId());
		}
	}
}
