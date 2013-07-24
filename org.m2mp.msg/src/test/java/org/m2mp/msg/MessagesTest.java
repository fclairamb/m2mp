/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.m2mp.msg.base.Message;
import org.m2mp.msg.base.MessageWrapper;

/**
 *
 * @author Florent Clairambault
 */
public class MessagesTest {

	@Test
	public void simpleSerDeser() {
		String serialized;
		{ // We create the message
			Message msg = new Message("me", "you", "my-love");
			Map<String, Object> ct = msg.getContent();
			ct.put("deep", "very");
			ct.put("sincere", "totally");
			{
				Map<String, Object> loc = new HashMap<>();
				loc.put("lat", 48.8);
				loc.put("lon", 2.5);
				ct.put("place", loc);
			}
			serialized = msg.serialize();
		}

		{ // We test it
			Message msg = Message.deserialize(serialized);
			Assert.assertEquals("me", msg.getFrom());
			Assert.assertEquals("you", msg.getTo());
			Assert.assertEquals("my-love", msg.getSubject());
			Map<String, Object> ct = msg.getContent();
			Assert.assertEquals("very", ct.get("deep"));
			Assert.assertEquals("totally", ct.get("sincere"));
			Map<String, Object> loc = (Map<String, Object>) ct.get("place");
			Assert.assertEquals(48.8, loc.get("lat"));
			Assert.assertEquals(2.5, loc.get("lon"));
		}
	}

	class SampleMessage {

		public static final String SUBJECT = "sample";
		private final Message msg;

		public SampleMessage(String from, String to) {
			msg = new Message(from, to, SUBJECT);
		}

		public SampleMessage(Message msg) {
			this.msg = msg;
		}
		private static final String PROP_NAME = "name";

		public void setName(String name) {
			msg.getContent().put(PROP_NAME, name);
		}

		public String getName() {
			return (String) msg.getContent().get("name");
		}

		public String serialize() {
			return msg.serialize();
		}
	}

	@Test
	public void messageSerDeser() {
		String serialized;
		{ // We create the message
			SampleMessage sm = new SampleMessage("me", "you");
			sm.setName("Micheline");
			serialized = sm.serialize();
		}

		{ // We test it
			Message msg = Message.deserialize(serialized);
			if (msg.getSubject().equals(SampleMessage.SUBJECT)) {
				SampleMessage sm = new SampleMessage(msg);
				Assert.assertEquals("Micheline", sm.getName());
			} else {
				throw new RuntimeException("Type " + msg.getSubject() + " is unknown !");
			}
		}
	}

	class SampleMessage2 extends MessageWrapper {

		public static final String SUBJECT = "sample";

		public SampleMessage2(String from, String to) {
			this(new Message(from, to, SUBJECT));
		}

		public SampleMessage2(Message msg) {
			super(msg);
		}
		private static final String PROP_NAME = "name";

		public void setName(String name) {
			msg.getContent().put(PROP_NAME, name);
		}

		public String getName() {
			return (String) msg.getContent().get("name");
		}
	}

	@Test
	public void messageWrapperSerDeser() {
		String serialized;
		{ // We create the message
			SampleMessage2 sm = new SampleMessage2("me", "you");
			sm.setName("Micheline");
			serialized = sm.serialize();
		}

		{ // We test it
			Message msg = Message.deserialize(serialized);
			if (msg.getSubject().equals(SampleMessage.SUBJECT)) {
				SampleMessage2 sm = new SampleMessage2(msg);
				Assert.assertEquals("Micheline", sm.getName());
			} else {
				throw new RuntimeException("Type " + msg.getSubject() + " is unknown !");
			}
		}
	}
}
