/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg;

import java.util.logging.Level;
import java.util.logging.Logger;
import ly.bit.nsq.exceptions.NSQException;

public class NSQTest {

	private boolean ok;

	public void testTransmission() throws NSQException, InterruptedException {
		final Client c1 = new Client("t1", "abc");

		final Client c2 = new Client("t2", "abc");

		final Object sync = new Object();

		c1.setHandler(new MessageHandler() {

			@Override
			public void handleMessage(Message msg) {
				switch (msg.getCall()) {
					case "req1":
						try {
							Message reply = msg.reply();
							reply.setCall("req2");
							c1.sendMessage(reply);
						} catch (NSQException ex) {
							Logger.getLogger(NSQTest.class.getName()).log(Level.SEVERE, null, ex);
						}
						break;
					case "end":
						ok = true;
						synchronized (sync) {
							sync.notify();
						}
						break;
				}
			}
		});

		c2.setHandler(new MessageHandler() {

			@Override
			public void handleMessage(Message msg) {
				if (msg.getCall().equals("req2")) {
					try {
						Message reply = msg.reply();
						reply.setCall("end");
						c2.sendMessage(reply);
					} catch (NSQException ex) {
						Logger.getLogger(NSQTest.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
			}
		});

		{ // We start the reaction chain
			Message msg = new Message("t1", "req1");
			c2.sendMessage(msg);
		}

		synchronized (sync) {
			sync.wait(180000);
		}

		if (!ok) {
			throw new RuntimeException("We failed !");
		}
	}

}
