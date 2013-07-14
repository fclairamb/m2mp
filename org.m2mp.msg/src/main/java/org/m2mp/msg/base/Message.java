package org.m2mp.msg.base;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author florent
 */
public class Message {

	private String from, to;
	private final String subject;
	private final Date date;
	private final Map<String, Object> content;

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

	public Message(String subject) {
		this(null, null, subject);
	}

	public static Message deserialize(String serialized) {
		Map<String, Object> obj = (Map<String, Object>) JSONValue.parse(serialized);
		String from = (String) obj.remove(PROP_FROM);
		String to = (String) obj.remove(PROP_TO);
		String subject = (String) obj.remove(PROP_SUBJECT);
		Date date = new Date((long) obj.remove(PROP_DATE));
		Map<String, Object> content = obj;
		return new Message(from, to, date, subject, content);
	}

	public Message setFrom(String f) {
		from = f;
		return this;
	}

	public String getFrom() {
		return from;
	}

	public Message setTo(String t) {
		to = t;
		return this;
	}

	public String getTo() {
		return to;
	}

	public String getSubject() {
		return subject;
	}

	public Date getDate() {
		return date;
	}

	public void setContext(String context) {
		if (context != null) {
			getContent().put(PROP_CONTEXT, context);
		}
	}

	public Message setContext() {
		setContext(UUID.randomUUID().toString());
		return this;
	}

	public String getContext() {
		return (String) getContent().get(PROP_CONTEXT);
	}

	public Map<String, Object> getContent() {
		return content;
	}

	public Message setContent(Map<String, Object> c) {
		content.clear();
		content.putAll(c);
		return this;
	}
	private static final String PROP_TO = "__to";
	private static final String PROP_FROM = "__from";
	private static final String PROP_SUBJECT = "__subject";
	private static final String PROP_DATE = "__date";
	private static final String PROP_CONTEXT = "__context";

	public String serialize() {
		Map<String, Object> obj = new HashMap<>();
		obj.put(PROP_FROM, from);
		obj.put(PROP_TO, to);
		obj.put(PROP_SUBJECT, subject);
		obj.put(PROP_DATE, date.getTime());
		obj.putAll(content);
		return JSONObject.toJSONString(obj);
	}

	public Message prepareReply() {
		Message reply = new Message(getTo(), getFrom(), getSubject());
		reply.setContext(getContext());
		return reply;
	}

	@Override
	protected Message clone() throws CloneNotSupportedException {
		return new Message(from, to, date, subject, content);
	}

	protected List<Message> clone(List<String> tos) {
		List<Message> msgs = new ArrayList<>(tos.size());
		for (String t : tos) {
			msgs.add(new Message(from, t, date, subject, content));
		}
		return msgs;
	}

	@Override
	public String toString() {
		return "FROM: " + getFrom() + ", TO: " + getTo() + ", SUBJECT: " + getSubject() + ", DATE: " + getDate() + ", CONTENT: " + getContent();
	}
}
