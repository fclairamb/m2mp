/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.svc;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.m2mp.msg.base.Message;

/**
 *
 * @author florent
 */
public abstract class MessagingServiceDaemon extends MessagingService implements Daemon {

	@Override
	public abstract String getMyQueueName();

	@Override
	public abstract void received(Message msg);

	@Override
	public void init(DaemonContext dc) throws DaemonInitException, Exception {
		super.start();
	}

	@Override
	public void stop() throws IOException {
		super.stop();
	}

	@Override
	public void destroy() {
		try {
			super.stop();
		} catch (IOException ex) {
			log.catching(ex);
		}
	}
}
