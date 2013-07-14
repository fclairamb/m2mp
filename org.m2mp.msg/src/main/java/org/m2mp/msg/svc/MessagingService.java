package org.m2mp.msg.svc;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.m2mp.msg.base.Message;
import org.m2mp.msg.base.MessageReceiver;
import org.m2mp.msg.base.MessagingClient;
import org.m2mp.msg.base.MessagingClientAsync;

/**
 * Abstract messaging service
 *
 * @author Florent Clairambault
 */
public abstract class MessagingService implements MessageReceiver {

	protected MessagingClientAsync clientAsync;
	protected final Logger log = LogManager.getLogger(getClass());

	/**
	 * Get the queue name of the messaging service
	 *
	 * @return Name of the queue of the messaging service
	 */
	public abstract String getMyQueueName();

	/**
	 * Get the queue name of the the server
	 *
	 * @return
	 */
	public String getServerQueueName() {
		return "localhost";
	}

	/**
	 * Start messaging
	 */
	public void start() throws Exception {
		clientAsync = new MessagingClientAsync(new MessagingClient(getServerQueueName(), getMyQueueName()), this);
		clientAsync.start();
		log.debug("Messaging service around queue \"{}\" Started !", getMyQueueName());
	}

	public void stop() throws IOException {
		clientAsync.stop();
	}

	public void send(Message msg) {
		clientAsync.send(msg);
	}

	protected boolean defaultMessageHandling(Message msg) throws Exception {
		switch (msg.getSubject()) {
			case StatusMessage.SUBJECT:
				StatusMessage m = new StatusMessage(msg);
				if (m.getType() == StatusMessage.Type.request) {
					clientAsync.send(m.reply(StatusMessage.Status.ok).getMessage());
					return true;
				}
		}
		return false;
	}

	@Override
	public void ended(boolean disconnected) {
		log.warn("Messaging queue around \"{}\" ended / disconnected={}", getMyQueueName(), disconnected);
		try {
			stop();
		} catch (IOException ex) {
			log.catching(ex);
		}
	}
}
