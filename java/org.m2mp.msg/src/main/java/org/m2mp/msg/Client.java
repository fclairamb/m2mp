package org.m2mp.msg;

import java.util.Map;
import java.util.TreeMap;
import ly.bit.nsq.NSQProducer;
import ly.bit.nsq.NSQReader;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.BasicLookupd;
import ly.bit.nsq.syncresponse.SyncResponseHandler;
import ly.bit.nsq.syncresponse.SyncResponseReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Client implements AutoCloseable {

	private final String topic, channel, nsqServer;
	private final NSQReader reader;
	private final TreeMap<String, NSQProducer> producers = new TreeMap<>();

	private MessageHandler handler;

	public Client(String topic, String channel) {
		this(topic, channel, "http://127.0.0.1:4161", "http://127.0.0.1:4151");
	}

	public Client(String topic, String channel, String lookupServer, String nsqServer) {
		this.topic = topic;
		this.channel = channel;
		this.nsqServer = nsqServer;

		reader = new SyncResponseReader(topic, channel, new SyncResponseHandler() {

			@Override
			public boolean handleMessage(ly.bit.nsq.Message m1) throws NSQException {
				try {
					handler.handleMessage(convertMessage(m1));
					return true;
				} catch (ParseException ex) {
					throw new NSQException(ex);
				}
			}
		});
		reader.addLookupd(new BasicLookupd(lookupServer));
	}

	public void setHandler(MessageHandler msgHandler) {
		this.handler = msgHandler;
	}

	private Message convertMessage(ly.bit.nsq.Message m1) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(new String(m1.getBody()));
		return new Message(obj);
	}

	public void sendMessage(Message m2) throws NSQException {
		{ // From checkup
			String from = m2.getFrom();

			if (from == null || from.startsWith(";")) {
				if (from == null) {
					from = "";
				}
				m2.setFrom(topic + "/" + channel + from);
			}
		}

		{ // Conversion
			String msgTopic = m2.getTopic();
			String msgBody = m2.toString();
			NSQProducer producer;
			synchronized (producers) {
				producer = producers.get(msgTopic);
				if (producer == null) {
					producer = new NSQProducer(nsqServer, msgTopic);
					producers.put(msgTopic, producer);
				}
			}
			producer.setTopic(msgTopic);
			producer.put(msgBody);
		}
	}

	@Override
	public void close() {
		reader.shutdown();
		synchronized (producers) {
			for (Map.Entry<String, NSQProducer> me : producers.entrySet()) {
				me.getValue().shutdown();
			}
		}
	}
}
