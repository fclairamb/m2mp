/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg;

import java.io.IOException;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.m2mp.msg.base.Message;
import org.m2mp.msg.base.MessagingClient;

/**
 *
 * @author florent
 */
public class Exchanges {

	@Test
	public void sendMessageToMyself() throws IOException, InterruptedException {
		MessagingClient c1 = new MessagingClient("localhost", "q1");
		MessagingClient c2 = new MessagingClient("localhost", "q2");

		c1.start();
		c2.start();

		String context;

		{ // c1 sends a message to c2
			Message m1 = new Message(c1.getQueueName(), c2.getQueueName(), "ping");
			m1.setContext();
			context = m1.getContext();
			m1.getContent().put("counter", 1);
			c1.send(m1);
		}

		{ // c2 reads the message and prepare a reply
			Message m1 = c2.recv();
			Assert.assertEquals(context, m1.getContext());
			Message m2 = m1.prepareReply();
			m2.getContent().put("counter", ((long) m1.getContent().get("counter")) + 1);
			c2.send(m2);
		}

		{ // c1 reads back the message
			Message m2 = c1.recv();
			Assert.assertEquals(2, (long) m2.getContent().get("counter"));
			Assert.assertEquals(context, m2.getContext());
		}

		c1.stop();
		c2.stop();
	}
}
