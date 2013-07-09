/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.base;

/**
 *
 * @author florent
 */
public class DeliveredMessage {

	private final long tag;
	private final Message msg;

	public DeliveredMessage(long tag, Message msg) {
		this.tag = tag;
		this.msg = msg;
	}
}
