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

/**
 *
 * @author florent
 */
public class Messages {

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
			Message msg = new Message(serialized);
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
}
