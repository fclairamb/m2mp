/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import java.util.Date;
import java.util.UUID;
import org.m2mp.db.registry.RegistryNode;

/**
 * Entity base class.
 *
 * An entity is only an extension of RegistryNode at this stage.
 *
 * @author Florent Clairambault
 */
public abstract class Entity {

	protected RegistryNode node;

	public Entity check() {
		node.check();
		return this;
	}

	protected void setProperty(String name, String value) {
		node.setProperty(name, value);
	}

	protected void setProperty(String name, long value) {
		node.setProperty(name, value);
	}

	protected void setProperty(String name, Date value) {
		node.setProperty(name, value);
	}

	protected void setProperty(String name, boolean value) {
		node.setProperty(name, value);
	}

	protected String getProperty(String name, String value) {
		return node.getProperty(name, value);
	}

	protected int getProperty(String name, int defaultValue) {
		return node.getProperty(name, defaultValue);
	}

	protected long getProperty(String name, long defaultValue) {
		return node.getProperty(name, defaultValue);
	}

	protected boolean getProperty(String name, boolean defaultValue) {
		return node.getProperty(name, defaultValue);
	}

	protected Date getPropertyDate(String name) {
		return node.getPropertyDate(name);
	}

	protected UUID getPropertyUUID(String name) {
		return node.getPropertyUUID(name);
	}

	public RegistryNode getNode() {
		return node;
	}

	public boolean exists() {
		return node.exists();
	}

	public Entity create() {
		node.create();
		return this;
	}

	public void delete() {
		node.delete();
	}
}
