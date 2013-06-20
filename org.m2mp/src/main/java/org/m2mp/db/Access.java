/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.m2mp.db.common.SessionWrapper;

/**
 *
 * @author florent
 */
public class Access {
	public static final SessionWrapper wrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), "ks1");
	public static final Session session = wrapper.session;
}
