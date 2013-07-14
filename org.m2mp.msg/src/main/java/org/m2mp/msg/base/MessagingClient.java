package org.m2mp.msg.base;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ControlClient handler
 *
 * @author Florent Clairambault / www.webingenia.com
 */
public final class MessagingClient {

	private final String serverName, myQueueName;
	private Channel channel;
	private QueueingConsumer receivingConsumer;
	protected final Logger log = LogManager.getLogger(getClass());

	public MessagingClient(String serverName, String myQueueName) {
		this.serverName = serverName;
		this.myQueueName = myQueueName;
	}

	public void start() throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(serverName);
		Connection connection = factory.newConnection();
		channel = connection.createChannel();
		receivingConsumer = new QueueingConsumer(channel);
		channel.queueDeclare(myQueueName, true, false, false, null);
		channel.basicConsume(myQueueName, false, receivingConsumer);
	}

	public void stop() throws IOException {
		receivingConsumer = null;
	}

	public synchronized void send(Message msg) throws IOException {
		log.debug("{} --> {}", this, msg);
		synchronized (channel) {
			channel.basicPublish("", msg.getTo(), MessageProperties.PERSISTENT_BASIC, msg.serialize().getBytes());
		}
	}

	public void send(List<Message> list) throws IOException {
		for (Message m : list) {
			send(m);
		}
	}

	public Message recv() throws IOException, InterruptedException {
		QueueingConsumer.Delivery delivery = receivingConsumer.nextDelivery();
		try {
			// We might throw something here...
			Message msg = Message.deserialize(new String(delivery.getBody()));
			log.debug("{} <-- {}", this, msg);
			return msg;
		} finally { // But we don't want to keep this bogus message for ever
			synchronized (channel) {
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		}
	}

	public String getQueueName() {
		return myQueueName;
	}

	public Message getMessage(String to, String subject) {
		return new Message(getQueueName(), to, subject);
	}

	@Override
	public String toString() {
		return "MessagingClient{queue=\"" + myQueueName + "\"}";
	}
}
