/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.base;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author florent
 */
public class AckableMessageRMQ implements AckableMessage {

	private final Channel channel;
	private final Delivery delivery;

	public AckableMessageRMQ(Channel c, Delivery d) {
		this.channel = c;
		this.delivery = d;
	}

	@Override
	public Message getMessage() {
		return Message.deserialize(new String(delivery.getBody()));
	}

	@Override
	public void ack() {
		try {
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		} catch (IOException ex) {
			Logger.getLogger(AckableMessageRMQ.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
