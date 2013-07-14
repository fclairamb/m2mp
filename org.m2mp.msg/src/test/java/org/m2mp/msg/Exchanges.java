/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.m2mp.msg.base.Message;
import org.m2mp.msg.base.MessageReceiver;

/**
 *
 * @author florent
 */
public class Exchanges implements MessageReceiver {

	@Test
	public void sendMessageToMyself() {
	}
	private final List<Message> received = new ArrayList<>();

	@Override
	public void received(Message msg) {
		received.add(msg);
	}

	@Override
	public void disconnected() {
		received.clear();
	}

	@Override
	public void ended() {
		received.clear();
	}
}
