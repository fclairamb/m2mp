/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.msg.base;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author florent
 */
public class Message {

	private final String from, to, subject;
	private final Date date;
	private final Map<String, Object> content;

	public Message(String serialized) {
		Map<String, Object> obj = (Map<String, Object>) JSONValue.parse(serialized);
		from = (String) obj.remove(PROP_FROM);
		to = (String) obj.remove(PROP_TO);
		subject = (String) obj.remove(PROP_SUBJECT);
		date = new Date((long) obj.remove(PROP_DATE));
		content = obj;
	}

	public Message(String from, String to, Date date, String subject, Map<String, Object> content) {
		this.from = from;
		this.to = to;
		this.date = date;
		this.subject = subject;
		this.content = content;
	}

	public Message(String from, String to, String subject) {
		this(from, to, new Date(), subject, new HashMap<String, Object>());
	}

	public String getFrom() {
		return from;
	}

	public String getTo() {
		return to;
	}

	public Date getDate() {
		return date;
	}

	public Map<String, Object> getContent() {
		return content;
	}
	private static final String PROP_TO = "__to";
	private static final String PROP_FROM = "__from";
	private static final String PROP_SUBJECT = "__subject";
	private static final String PROP_DATE = "__date";

	public String serialize() {
		Map<String, Object> obj = new HashMap<>();
		obj.put(PROP_FROM, from);
		obj.put(PROP_TO, to);
		obj.put(PROP_SUBJECT, subject);
		obj.put(PROP_DATE, date.getTime());
		obj.putAll(content);
		return JSONObject.toJSONString(obj);
	}
}
