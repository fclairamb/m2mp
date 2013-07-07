/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.ts;

import java.util.Date;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author florent
 */
public class TimedData {

	private final String id;
	private final String type;
	private final Date date;
	private final String json;

	// <editor-fold defaultstate="collapsed" desc="JSON constructors">
	public TimedData(String id, String json) {
		this(id, null, json);
	}

	public TimedData(String id, String type, String json) {
		this(id, type, new Date(), json);
	}

	public TimedData(String id, String type, Date date, String json) {
		this.id = id;
		this.type = type;
		this.date = date;
		this.json = json;
	}
	// </editor-fold>

	// <editor-fold defaultstate="collapsed" desc="Map constructors">
	public TimedData(String id, Map<String, Object> map) {
		this(id, null, map);
	}

	public TimedData(String id, String type, Map<String, Object> map) {
		this(id, type, new Date(), map);
	}

	public TimedData(String id, String type, Date date, Map<String, Object> map) {
		this(id, type, date, JSONObject.toJSONString(map));
	}
	// </editor-fold>

	// <editor-fold defaultstate="collapsed" desc="Access methods">
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

	public Object getJsonObject() {
		return JSONValue.parse(getJson());
	}

	public Map<String, Object> getJsonMap() {
		return (Map<String, Object>) getJsonObject();
	}
	// </editor-fold>
}
