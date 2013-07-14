package org.m2mp.msg.base;

import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author florent
 */
public class MessagingClientAsync implements Runnable {

	private final MessagingClient client;
	private MessageReceiver receiver;
	private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
	protected final Logger log = LogManager.getLogger(getClass());

	public MessagingClientAsync(MessagingClient client, MessageReceiver receiver) {
		this.client = client;
	}

	public void start() throws IOException {
		client.start();

		Thread thread = new Thread(this);
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(true);
	}

	public void stop() throws IOException {
		executor.shutdown();
		client.stop();
	}

	@Override
	public void run() {
		boolean disconnected = false;
		try {
			while (true) {
				try {
					receiver.received(client.recv());
				} catch (ShutdownSignalException ex) {
					disconnected = true;
				} catch (Exception ex) {
					log.catching(ex);
				}
			}
		} catch (Exception ex) {
			log.catching(ex);
		} finally {
			try {
				client.stop();
			} catch (IOException ex) {
				log.catching(ex);
			}
			receiver.ended(disconnected);
		}
	}

	public void sendAsync(String queueName, Message msg) throws Exception {
		executor.submit(new SendMessageTask(queueName, msg), 0);
	}

	public void sendAsync(Message msg) {
		executor.submit(new SendMessageTask(msg));
	}

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
				client.send(msg.setTo(queueName));
			} catch (Exception ex) {
				int att = attemptNb + 1;
				if (att < 5) {
					executor.schedule(new SendMessageTask(queueName, msg, att), att * 2, TimeUnit.SECONDS);
				}
			}
		}
	}
}
