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
 * @author Florent Clairambault
 */
public class MessagingClientAsync implements Runnable {

	private final MessagingClient client;
	private final MessageReceiver receiver;
	private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
	protected final Logger log = LogManager.getLogger(getClass());

	public MessagingClientAsync(MessagingClient client, MessageReceiver receiver) {
		this.client = client;
		this.receiver = receiver;
	}

	public void start() throws IOException {
		client.start();

		Thread thread = new Thread(this);
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(true);
		thread.start();
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
				receiver.received(client.recv());
			}
		} catch (Exception ex) {
			if (ex instanceof ShutdownSignalException) {
				disconnected = true;
			}
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

	public void send(Message msg) {
		executor.schedule(new SendMessageTask(msg), 0, TimeUnit.SECONDS);
	}

	private class SendMessageTask implements Runnable {

		private final Message msg;
		private final int attemptNb;

		public SendMessageTask(Message msg) {
			this(msg, 0);
		}

		private SendMessageTask(Message msg, int attemptNb) {
			this.msg = msg;
			this.attemptNb = attemptNb;
		}

		@Override
		public void run() {
			try {
				client.send(msg);
			} catch (Exception ex) {
				int att = attemptNb + 1;
				if (att < 5) {
					executor.schedule(new SendMessageTask(msg, att), att * 2, TimeUnit.SECONDS);
				}
			}
		}
	}
}
