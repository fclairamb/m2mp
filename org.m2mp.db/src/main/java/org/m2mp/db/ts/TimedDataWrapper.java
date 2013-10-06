package org.m2mp.db.ts;

import com.datastax.driver.core.utils.UUIDs;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Timed data wrapper.
 * <p/>
 * We have the same data as the TimedData class except we are working in JSON only, we can modify the content efficiently
 * and we can save it to modify an existing time serie element (a timedData).
 */
public class TimedDataWrapper {

    private final String id;
    private final String type;
    private final UUID date;
    private Map<String, Object> map;
    private boolean mod;

    public TimedDataWrapper(String id, String type, UUID date, Map<String, Object> map) {
        if (id != null) {
            int p = id.indexOf('!');
            if (p != -1) {
                String srcId = id;
                id = srcId.substring(0, p);
                type = srcId.substring(p + 1);
            }
        }

        this.id = id;
        this.type = type;
        this.date = date;
        this.map = map;
        this.mod = true;
    }

    public TimedDataWrapper(String type, UUID date, Map<String, Object> map) {
        this(null, type, date, map);
    }

    public TimedDataWrapper(String type, Date date, Map<String, Object> map) {
        this(null, type, new UUID(UUIDs.startOf(date.getTime()).getMostSignificantBits(), System.nanoTime()), map);
    }

    public TimedDataWrapper(String type) {
        this(type, new Date(), new TreeMap<String, Object>());
    }

    public TimedDataWrapper(TimedData td) {
        this(td.getId(), td.getType(), td.getDateUUID(), td.getJsonMap());
    }

    public TimedDataWrapper(String id, String type, Map<String, Object> map) {
        this(id, type, new UUID(UUIDs.startOf(System.currentTimeMillis()).getMostSignificantBits(), System.nanoTime()), map);
    }

    public TimedDataWrapper(String id, String type) {
        this(id, type, new TreeMap<String, Object>());
    }

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

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
        modified();
    }

    public void set(String name, Object obj) {
        map.put(name, obj);
        modified();
    }

    public List<String> getStrings(String name) {
        return (List<String>) map.get(name);
    }

    public void set(String name, Iterable<String> many) {
        map.put(name, many);
        modified();
    }

    public String getString(String name) {
        return (String) map.get(name);
    }

    public Long getLong(String name) {
        Object obj = map.get(name);
        if (obj instanceof Long) // New format
            return (Long) obj;
        else if (obj instanceof String) // Old format
            return Long.parseLong((String) obj);
        else
            return null;
    }

    public Double getDouble(String name) {
        Object obj = map.get(name);
        if (obj instanceof Double) // New format
            return (Double) obj;
        else if (obj instanceof String) // Old format
            return Double.parseDouble((String) obj);
        else
            return null;
    }

    public Boolean getBoolean(String name) {
        Object obj = map.get(name);
        if (obj instanceof Boolean) // New format
            return (Boolean) obj;
        else if (obj instanceof String) // Old format
            return Boolean.parseBoolean((String) obj);
        else
            return null;
    }

    public String get(String name, String defaultValue) {
        String value = getString(name);
        return value != null ? value : defaultValue;
    }

    public long get(String name, long defaultValue) {
        Long value = getLong(name);
        return value != null ? value : defaultValue;
    }

    public int get(String name, int defaultValue) {
        return (int) get(name, (long) defaultValue);
    }

    public double get(String name, double defaultValue) {
        Double value = getDouble(name);
        return value != null ? value : defaultValue;
    }

    public boolean get(String name, boolean defaultValue) {
        Boolean value = getBoolean(name);
        return value != null ? value : defaultValue;
    }

    public void modified() {
        mod = true;
    }

    public String getJson() {
        return JSONObject.toJSONString(map);
    }

    public void save() {
        if (mod) {
            // We don't need to re-read the data, everything is on the map
            TimeSerie.save(this);
        }
    }

    public void delete() {
        TimeSerie.delete(this);
    }

    public String toString() {
        return "TDW{id=\"" + id + "\",type=\"" + type + "\",map=" + map + "}";
    }

    public static Iterable<TimedDataWrapper> iter(final Iterable<TimedData> iterable) {
        return new Iterable<TimedDataWrapper>() {
            @Override
            public Iterator<TimedDataWrapper> iterator() {
                return new Iterator<TimedDataWrapper>() {

                    private final Iterator<TimedData> iterator = iterable.iterator();

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public TimedDataWrapper next() {
                        return new TimedDataWrapper(iterator.next());
                    }

                    @Override
                    public void remove() {
                        iterator().remove();
                    }
                };
            }
        };
    }
}
