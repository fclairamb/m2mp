package org.m2mp.msg;

import java.util.ArrayList;
import java.util.List;
import ly.bit.nsq.exceptions.NSQException;

public class SharedClient {

	private static Client client;
	private static final MultiMessageHandler handler = new MultiMessageHandler();
	private static final List<MessageHandler> handlers = new ArrayList<>();

	private static class MultiMessageHandler implements MessageHandler {

		@Override
		public void handleMessage(Message msg) {
			synchronized (handlers) {
				MessageHandler toForget = null;
				for (MessageHandler h : handlers) {
					try {
						h.handleMessage(msg);
					} catch (ForgetMeException e) {
						toForget = h;
					} catch (Exception ex) {
						// This shouldn't happen
						ex.printStackTrace();
					}
				}
				if (toForget != null) {
					handlers.remove(toForget);
				}
			}
		}
	}

	public static class ForgetMeException extends RuntimeException {
	}

	public synchronized static void setup(String topic, String channel, String lookupServer, String nsqServer) {
		setClient(new Client(topic, channel, lookupServer, nsqServer));
	}

	public synchronized static void setup(String topic, String channel) {
		setClient(new Client(topic, channel));
	}

	private static void setClient(Client c) {
		stop();
		client = c;
		client.setHandler(handler);
	}

	public static void addHandler(MessageHandler hdl) {
		synchronized (handlers) {
			handlers.add(hdl);
		}
	}

	public static void removeHandler(MessageHandler hdl) {
		synchronized (handlers) {
			handlers.remove(hdl);
		}
	}

	public synchronized static void send(Message msg) throws NSQException {
		if (client != null) {
			client.sendMessage(msg);
		}
	}

	public synchronized static void stop() {
		if (client != null) {
			client.close();
			client = null;
		}
	}
}
