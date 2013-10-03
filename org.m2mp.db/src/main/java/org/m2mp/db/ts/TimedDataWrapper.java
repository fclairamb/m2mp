package org.m2mp.db.ts;

import com.datastax.driver.core.utils.UUIDs;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

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
        this.id = id;
        this.type = type;
        this.date = date;
        this.map = map;
        this.mod = true;
    }

    public TimedDataWrapper(TimedData td) {
        this(td.getId(), td.getType(), td.getDateUUID(), td.getJsonMap());
    }

    public TimedDataWrapper(String id, String type, Map<String, Object> map) {
        this(id, type, new UUID(UUIDs.startOf(System.currentTimeMillis()).getMostSignificantBits(), System.nanoTime()), map);
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

    public Iterable<String> getStrings(String name) {
        return (Iterable<String>) map.get(name);
    }

    public void set(String name, Iterable<String> many) {
        map.put(name, many);
    }

    public String getString(String name) {
        return (String) map.get(name);
    }

    public Long getLong(String name) {
        return (Long) map.get(name);
    }

    public Double getDouble(String name) {
        return (Double) map.get(name);
    }

    public String get(String name, String defaultValue) {
        String value = getString(name);
        return value != null ? value : defaultValue;
    }

    public long get(String name, long defaultValue) {
        Long value = getLong(name);
        return value != null ? value : defaultValue;
    }

    public double get(String name, double defaultValue) {
        Double value = getDouble(name);
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
