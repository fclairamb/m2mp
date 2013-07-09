package org.m2mp.msg.base;

import com.rabbitmq.client.*;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ControlClient handler
 *
 * @author Florent Clairambault / www.webingenia.com
 */
public final class MessagingClient implements Runnable {

	private final String serverName, myQueueName;
	private final Thread thread;
	private final MessageReceiver receiver;
	private final ScheduledExecutorService executor;
	private Channel channel;
	private QueueingConsumer receivingConsumer;
	protected final Logger log = LogManager.getLogger(getClass());

	private class SendMessageTask extends TimerTask {

		private final String queueName;
		private final Message msg;
		private final int attemptNb;
		
		public SendMessageTask(Message msg) {
			this(null, msg, 0);
		}

		public SendMessageTask(String queueName, Message msg) {
			this(queueName, msg, 0);
		}

		public SendMessageTask(String queueName, Message msg, int attemptNb) {
			assert (queueName != null);
			this.queueName = queueName;
			this.msg = msg;
			this.attemptNb = attemptNb;
		}

		@Override
		public void run() {
			try {
				actualSend(msg.setTo(queueName));
			} catch (Exception ex) {
				int att = attemptNb + 1;
				if (att < 5) {
					executor.schedule(new SendMessageTask(queueName, msg, att), att * 2, TimeUnit.SECONDS);
				}
			}
		}
	}

	public MessagingClient(String serverName, String myQueueName, MessageReceiver receiver) {
		this.serverName = serverName;
		this.myQueueName = myQueueName;
		this.receiver = receiver;
		this.executor = new ScheduledThreadPoolExecutor(1);

		thread = new Thread(this, "RMQ_Receiver_" + myQueueName);
		thread.setDaemon(true);
		thread.setPriority(Thread.MIN_PRIORITY);
	}

	public void start() throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(serverName);
		Connection connection = factory.newConnection();
		channel = connection.createChannel();
		receivingConsumer = new QueueingConsumer(channel);
		channel.queueDeclare(myQueueName, true, false, false, null);
		channel.basicConsume(myQueueName, false, receivingConsumer);
		thread.start();
	}

	public void stop() throws IOException {
		receivingConsumer = null;
		executor.shutdown();
	}

	@Override
	public void run() {
		try {
			while (receivingConsumer != null) {
				try {
					Delivery delivery = receivingConsumer.nextDelivery();
					Message msg = Message.deserialize(new String(delivery.getBody()));
					receiver.received(msg);
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				} catch (ShutdownSignalException ex) {
					receiver.disconnected();
					receivingConsumer = null;
				} catch (Exception ex) {
				}
			}
			channel.close();
			channel = null;
		} catch (IOException ex) {
			log.catching(ex);
		} finally {
			receiver.ended();
		}
	}

	public void sendAsync(String queueName, Message msg) throws Exception {
		msg.setFrom(myQueueName);
		executor.submit(new SendMessageTask(queueName, msg), 0);
	}
	
	public void sendAsync(Message msg) {
		executor.submit(new SendMessageTask(msg));
	}
	
	public void send(Message msg ) throws Exception {
		msg.setFrom(myQueueName);
		actualSend(msg);
	}

	private void actualSend(Message msg) throws Exception {
		synchronized (this) {
			channel.basicPublish("", msg.getTo(), MessageProperties.PERSISTENT_BASIC, msg.serialize().getBytes());
		}
	}

	@Override
	public String toString() {
		return "MessagingClient{queue=\"" + myQueueName + "\"}";
	}
}
