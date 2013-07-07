package org.m2mp.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.m2mp.db.common.SessionWrapper;

/**
 *
 * @author Florent Clairambault
 */
public class Shared {

	private static byte[] UUIDToBytes(UUID u) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(u.getMostSignificantBits());
		bb.putLong(u.getLeastSignificantBits());
		return bb.array();
	}
	private static String name = "m2mp_v2";

	public static void setKeySpace(String name) {
		Shared.name = name;
	}

	public static void switchToProduction() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name);
	}

	public static void switchToTest() {
		sessionWrapper = new SessionWrapper(Cluster.builder().addContactPoint("localhost").build(), name + "_test");
	}
	public static SessionWrapper sessionWrapper;

	public static Session db() {
		return sessionWrapper.getSession();
	}

	public static KeyspaceMetadata dbMgmt() {
		return sessionWrapper.getKs();
	}
}
