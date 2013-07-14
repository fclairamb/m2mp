/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.base;

/**
 *
 * @author florent
 */
public interface AckableMessage {
	Message getMessage();
	void ack();
}
