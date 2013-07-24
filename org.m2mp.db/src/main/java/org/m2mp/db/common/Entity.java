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

	protected void setProperty(String name, UUID id) {
		node.setProperty(name, id);
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

	protected void delProperty(String name) {
		node.delProperty(name);
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
	/**
	 * Property: Date of deletion.
	 *
	 * This property might be of importance when you want to remove the entities
	 * that haven't been used for quite some time first.
	 */
	private static final String PROPERTY_DELETED_DATE = ".deleted_date";

	public void delete() {
		node.delete();
		setProperty(PROPERTY_DELETED_DATE, System.currentTimeMillis());
	}

	public boolean deleted() {
		return node.deleted();
	}

	public long deletedTime() {
		return getProperty(PROPERTY_DELETED_DATE, 0);
	}

	public void undelete() {
		node.check();
		delProperty(PROPERTY_DELETED_DATE);
	}
	private static final String PROP_VERSION = ".version";

	protected abstract int getObjectVersion();

	public boolean versionOutdated() {
		return getObjectVersion() > getProperty(PROP_VERSION, 0);
	}

	public void versionUpdate() {
		// If the deletedTime property wasn't applied yet
		if (deleted() && deletedTime() == 0) {
			delete();
		}
	}

	public boolean versionCheck() {
		if (versionOutdated()) {
			versionUpdate();
			return true;
		} else {
			return false;
		}
	}
}
