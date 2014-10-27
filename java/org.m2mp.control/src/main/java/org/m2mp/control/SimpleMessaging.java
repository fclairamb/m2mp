package org.m2mp.control;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import ly.bit.nsq.exceptions.NSQException;
import org.m2mp.msg.Message;
import org.m2mp.msg.SharedClient;

/**
 * Dead simple messaging wrapper.
 *
 * @author florent
 */
public class SimpleMessaging {

	public static void requestReceiversSendSettings(UUID deviceId) {
		send(new Message("receivers;device_id=" + deviceId, "send_settings"));
	}

	public static void requestReceiversSendCommands(UUID deviceId) {
		send(new Message("receivers;device_id=" + deviceId, "send_commands"));
	}

	public static void requestReceiversDisconnect(String target, int connectionId) {
		send(new Message(target + ";connection_id=" + connectionId, "disconnect"));
	}

	private static void send(Message msg) {
		try {
			SharedClient.send(msg);
		} catch (NSQException ex) {
			Logger.getLogger(SimpleMessaging.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
