package org.m2mp.msg.base;


/**
 *
 * @author Florent Clairambault
 */
public interface MessageReceiver {
    void received( Message msg);

	public void disconnected();

	public void ended();
}
