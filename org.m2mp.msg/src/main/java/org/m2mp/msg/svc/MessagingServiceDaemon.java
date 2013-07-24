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
import org.apache.commons.daemon.DaemonController;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.m2mp.msg.base.Message;

/**
 *
 * @author Florent Clairambault
 */
public abstract class MessagingServiceDaemon extends MessagingService implements Daemon {

	@Override
	public abstract String getMyQueueName();

	@Override
	public abstract void received(Message msg);
	DaemonContext daemonContext;

	@Override
	public void init(DaemonContext dc) throws DaemonInitException, Exception {
		this.daemonContext = dc;
	}

	@Override
	public void start() throws Exception {
		log.warn("Service \"{}\" starting...", getMyQueueName());
		super.start();
	}

	@Override
	public void stop() throws IOException {
		log.warn("Service \"{}\" stopping...", getMyQueueName());
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

	public static void mainHandling(final String[] args, MessagingServiceDaemon msd) throws Exception {
		msd.init(new DaemonContext() {
			@Override
			public DaemonController getController() {
				return new DaemonController() {
					protected final org.apache.logging.log4j.Logger log = LogManager.getLogger(getClass());

					@Override
					public void shutdown() throws IllegalStateException {
						log.warn("DC: shutdown");
					}

					@Override
					public void reload() throws IllegalStateException {
						log.warn("DC: reload");
					}

					@Override
					public void fail() throws IllegalStateException {
						log.warn("DC: fail");
					}

					@Override
					public void fail(String string) throws IllegalStateException {
						log.warn("DC: fail {}", string);
					}

					@Override
					public void fail(Exception excptn) throws IllegalStateException {
						log.warn("DC: fail {}", excptn);
					}

					@Override
					public void fail(String string, Exception excptn) throws IllegalStateException {
						log.warn("DC: fail {}:{}", string, excptn);
					}
				};
			}

			@Override
			public String[] getArguments() {
				return args;
			}
		});

		msd.start();
	}
}
