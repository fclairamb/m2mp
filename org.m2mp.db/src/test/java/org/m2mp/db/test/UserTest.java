/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.util.UUID;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DBAccess;
import org.m2mp.db.entity.Domain;
import org.m2mp.db.entity.User;
import org.m2mp.db.ts.TimeSerie;

/**
 *
 * @author florent
 */
public class UserTest {

	private static DBAccess db;

	@BeforeClass
	public static void setUpClass() {
		db = DBAccess.getOrCreate("ks_test");
		try {
			db.execute("drop table " + Domain.TABLE + ";");
			db.execute("drop table " + User.TABLE + ";");
		} catch (Exception ex) {
		}
		User.prepareTable(db);
	}

	@Test
	public void create() {
		// Domain
		Domain domain = Domain.create(db, "domain");

		{
			User user1 = User.get(db, "user1");
			Assert.assertNull(user1);
		}
		{
			User user1 = User.create(db, "name", domain);
			User user2 = User.get(db, "name");
			Assert.assertNotNull(user1.getId());
			Assert.assertEquals(user1.getId(), user2.getId());
		}
	}
}
