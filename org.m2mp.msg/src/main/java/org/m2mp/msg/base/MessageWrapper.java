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

	public MessageWrapper prepareReply() {
		return new MessageWrapper(msg.prepareReply());
	}

	public Message getMessage() {
		return msg;
	}

	public String serialize() {
		return msg.serialize();
	}
}
