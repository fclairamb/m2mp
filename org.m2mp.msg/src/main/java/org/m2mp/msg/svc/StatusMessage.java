/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.svc;

import org.m2mp.msg.base.Message;
import org.m2mp.msg.base.MessageWrapper;

/**
 *
 * @author Florent Clairambault
 */
public class StatusMessage extends MessageWrapper {

	public enum Type {

		request,
		response
	}

	public enum Status {

		ok,
		nok
	}
	public static final String SUBJECT = "status";
	private static final String PROP_TYPE = "type";
	private static final String PROP_STATUS = "status";

	public Type getType() {
		return Type.valueOf((String) msg.getContent().get(PROP_TYPE));
	}

	public void setType(Type type) {
		msg.getContent().put(PROP_TYPE, type.name());
	}

	public Status getStatus() {
		return Status.valueOf((String) msg.getContent().get(PROP_STATUS));
	}

	public void setStatus(Status status) {
		msg.getContent().put(PROP_STATUS, status.name());
	}

	public StatusMessage(Message msg) {
		super(msg);
	}

	public StatusMessage() {
		super(new Message(SUBJECT));
	}
	
	public StatusMessage reply( Status s) {
		return null;
	}
}
