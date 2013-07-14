package org.m2mp.services.ping;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.m2mp.msg.base.Message;
import org.m2mp.msg.svc.MessagingServiceDaemon;

/**
 * Hello world!
 *
 */
public class App extends MessagingServiceDaemon {

	public static void main(String[] args) throws Exception {
		MessagingServiceDaemon.mainHandling(args, new App());
	}

	@Override
	public String getMyQueueName() {
		return "ping";
	}

	@Override
	public void received(Message msg) {
		log.warn("Received: {}", msg);
		try {
			if (defaultMessageHandling(msg)) {
				return;
			}
		} catch (Exception ex) {
			log.catching(ex);
		}
		send(msg.prepareReply().setContent(msg.getContent()));
	}
}
