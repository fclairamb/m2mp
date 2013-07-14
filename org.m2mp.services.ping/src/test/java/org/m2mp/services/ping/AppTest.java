package org.m2mp.services.ping;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.m2mp.msg.base.Message;
import org.m2mp.msg.base.MessagingClient;

/**
 * Unit test for simple App.
 */
public class AppTest {

	@Test
	public void sendPing() throws IOException, InterruptedException {
		MessagingClient clt = new MessagingClient("localhost", "ping_tester");
		clt.start();
		Message m1 = clt.getMessage("ping", "ping").setContext();
		String context = m1.getContext();
		clt.send(m1);
		Message m2 = clt.recv();
		Assert.assertEquals(context, m2.getContext());
	}
}
