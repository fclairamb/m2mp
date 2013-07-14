/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.es;

import com.google.gson.JsonObject;
import java.text.SimpleDateFormat;

/**
 *
 * @author Florent Clairambault
 */
public interface EsIndexable {

	public JsonObject getEsIndexableContent();

	public String getEsIndexName();

	public String getEsRouting();

	public String getEsDocId();

	public String getEsDocType();
	public static final String PROPERTY_ES_VERSION = "_version";
	public static final String PROPERTY_ES_UPDATE = "_update";
	public static final String PROPERTY_ES_DOMAIN = "_domain";
	public static final String PROPERTY_ES_CREATED = "created";
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	//FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss", TimeZone.getTimeZone("UTC"));
}
