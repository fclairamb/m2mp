/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.ts;

import java.util.Date;
import java.util.Iterator;

/**
 *
 * @author Florent Clairambault
 */
public class GetDataIterable implements Iterable<TimedData> {

	private final String id;
	private final String type;
	private final Date dateBegin;
	private final Date dateEnd;
	private final boolean orderAsc;

	public GetDataIterable(String id, String type, Date dateBegin, Date dateEnd, boolean orderAsc) {
		this.id = id;
		this.type = type;
		this.dateBegin = dateBegin;
		this.dateEnd = dateEnd;
		this.orderAsc = orderAsc;
	}

	@Override
	public Iterator<TimedData> iterator() {
		return new GetDataIterator(id, type, dateBegin, dateEnd, orderAsc);
	}
}
