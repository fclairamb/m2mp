/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db;

import java.util.Date;

/**
 *
 * @author florent
 */
public class TimedData {

	private final String id;
	private final String type;
	private final Date date;
	private final String json;

	public TimedData(String id, String type, String json) {
		this.id = id;
		this.type = type;
		this.date = new Date();
		this.json = json;
	}

	public TimedData(String id, String type, Date date, String json) {
		this.id = id;
		this.type = type;
		this.date = date;
		this.json = json;
	}

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public Date getDate() {
		return date;
	}

	public String getJson() {
		return json;
	}
}
