/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.base;

/**
 *
 * @author florent
 */
public class MessageWrapper {

	protected final Message msg;

	public MessageWrapper(Message msg) {
		this.msg = msg;
	}

	public Message prepareReply() {
		Message reply = new Message(msg.getTo(), msg.getFrom(), msg.getSubject());
		reply.setContext(msg.getContext());
		return msg;
	}

	public Message getMessage() {
		return msg;
	}

	public String serialize() {
		return msg.serialize();
	}
}
