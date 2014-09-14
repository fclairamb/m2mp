package org.m2mp.msg;

import java.util.Date;
import org.json.simple.JSONObject;

public final class Message {

	private static final String FIELD_FROM = "_from",
			FIELD_TO = "_to",
			FIELD_CALL = "_call",
			FIELD_TIME = "_time";

	public static final String TOPIC_GENERAL_EVENTS = "events",
			TOPIC_STORAGE = "storage",
			TOPIC_ALERTS = "alerts",
			TOPIC_RECEIVERS = "receivers";

	private final JSONObject obj;

	public Message(JSONObject obj) {
		this.obj = obj;
	}

	public Message(String to, String call) {
		this.obj = new JSONObject();
		setTo(to);
		setCall(call);
		setDate();
	}

	public String getFrom() {
		return (String) obj.get(FIELD_FROM);
	}

	public void setFrom(String from) {
		obj.put(FIELD_FROM, from);
	}

	public String getTo() {
		return (String) obj.get(FIELD_TO);
	}

	public void setTo(String to) {
		obj.put(FIELD_TO, to);
	}

	public String getCall() {
		return (String) obj.get(FIELD_CALL);
	}

	public void setCall(String call) {
		obj.put(FIELD_CALL, call);
	}

	public Date getDate() {
		return new Date((long) obj.get(FIELD_TIME) * 1000);
	}

	public void setDate(Date date) {
		this.obj.put(FIELD_TIME, date.getTime() / 1000);
	}

	public void setDate() {
		setDate(new Date());
	}

	public JSONObject obj() {
		return obj;
	}

	public String getTopic() {
		String to = getTo();
		to = to.split("/", 2)[0];
		to = to.split(";", 2)[0];
		return to;
	}

	public Message reply() {
		Message msg = new Message(new JSONObject());
		msg.setFrom(getTo());
		msg.setTo(getFrom());
		msg.setDate();
		return msg;
	}

	@Override
	public String toString() {
		return obj.toJSONString();
	}
}
