/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.ts;

import com.datastax.driver.core.utils.UUIDs;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author florent
 */
public class TimedData {

	private final String id;
	private final String type;
	private final UUID date;
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
		this.date = new UUID(UUIDs.startOf(date.getTime()).getMostSignificantBits(), System.nanoTime());
		this.json = json;
	}

	public TimedData(String id, String type, UUID date, String json) {
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

	public UUID getDateUUID() {
		return date;
	}

	public Date getDate() {
		return new Date(UUIDs.unixTimestamp(date));
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
