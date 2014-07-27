package org.m2mp.db.ts;

import java.util.Date;
import java.util.Iterator;

public class TSPeriodIterable implements Iterable<String> {

    private final String id, type;
    private final Date begin, end;
    private final boolean inverted;

    public TSPeriodIterable(String id, String type, Date begin, Date end, boolean inverted) {
        this.id = id;
        this.type = type;
        this.begin = begin;
        this.end = end;
        this.inverted = inverted;
    }

    @Override
    public Iterator<String> iterator() {
        return new TSPeriodIterator(id, type, begin, end, inverted);
    }
}
