package org.m2mp.db.ts;

import com.datastax.driver.core.utils.UUIDs;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Timed data.
 *
 * @author Florent Clairambault
 */
public class TimedData {

	private final String id;
	private final String type;
	private final UUID date;
	private final String data;

	// <editor-fold defaultstate="collapsed" desc="JSON constructors">
	public TimedData(String id, String data) {
		this(id, null, data);
	}

	public TimedData(String id, String type, String data) {
		this(id, type, new Date(), data);
	}

	public TimedData(String id, String type, Date date, String data) {
		this.id = id;
		this.type = type;
		this.date = new UUID(UUIDs.startOf(date.getTime()).getMostSignificantBits(), System.nanoTime());
		this.data = data;
	}

	public TimedData(String id, String type, UUID date, String json) {
		this.id = id;
		this.type = type;
		this.date = date;
		this.data = json;
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

	public String getData() {
		return data;
	}

	public Object getJsonObject() {
		return JSONValue.parse(getData());
	}

	public Map<String, Object> getJsonMap() {
		return (Map<String, Object>) getJsonObject();
	}

	// </editor-fold>
	@Override
	public String toString() {
		return "TimedDate[" + id + "/" + type + "][" + getDate() + "] = " + data;
	}
}
