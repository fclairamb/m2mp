/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.launcher;

import java.util.Objects;

/**
 *
 * @author florent
 */
public class ConfigService implements Comparable<ConfigService> {

	public String queueName;
	public boolean disabled = false;
	public String fileName;
	public String className;
	public boolean restartOnFailure = true;
	public int delayBetweenChecks = 30;
	public long lastModified;
	

	ConfigService(String queueName, String fileName) {
		this.queueName = queueName;
		this.fileName = fileName;
	}
	
	public boolean check() {
		boolean r = false;
		if ( delayBetweenChecks == 0 ) {
			r = true;
			delayBetweenChecks = 30;
		}
		return r;
	}

	// <editor-fold defaultstate="collapsed" desc="non value code">
	@Override
	public String toString() {
		return "ConfigService{queueName=\"" + queueName + "\", fileName=\"" + fileName + "\"}";
	}

	@Override
	public int compareTo(ConfigService o) {
		return queueName.compareTo(o.queueName);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ConfigService) {
			return queueName.equals(((ConfigService) obj).queueName);
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 97 * hash + Objects.hashCode(this.queueName);
		return hash;
	}
	// </editor-fold>
}
