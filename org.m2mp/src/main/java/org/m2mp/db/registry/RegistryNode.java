/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.registry;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.m2mp.db.Shared;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author florent
 *
 * The registry node is a kind of "does it all" class. It's very convenient to
 * store pretty much anything.
 */
public class RegistryNode {

	protected String path;

	public RegistryNode(String path) {
		this.path = path;
	}

	public void check() {
		int status = getStatus();
		if (status == 0) {
			create();
		}
	}

	public void create() {
		
		
	}
	private static final String TABLE_REGISTRY = "RegistryNode";

	public static void prepareTable() {
		TableCreation.checkTable(new TableIncrementalDefinition() {
			public String getTableDefName() {
				return TABLE_REGISTRY;
			}

			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<TableIncrementalDefinition.TableChange>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_REGISTRY + " ( path text PRIMARY KEY, values map<text,text>, status int );"));
				list.add(new TableIncrementalDefinition.TableChange(2, "CREATE TABLE " + TABLE_REGISTRY + "Children ( path text, name text, PRIMARY KEY( path, name ) ) WITH CLUSTERING ORDER BY ( name ASC );"));
				list.add(new TableIncrementalDefinition.TableChange(3, "CREATE TABLE " + TABLE_REGISTRY + "Data ( path text, block int, data blob, PRIMARY KEY( path, block ) ) WITH CLUSTERING ORDER BY ( block ASC );"));
				return list;
			}

			public int getTableDefVersion() {
				return 1;
			}
		});
	}
	// <editor-fold defaultstate="collapsed" desc="Children management">
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Status management">
	private static final int STATUS_DELETED = 5;
	private static final int STATUS_CREATED = 100;
	private static PreparedStatement _reqGetStatus;

	private static PreparedStatement reqGetStatus() {
		if (_reqGetStatus == null) {
			_reqGetStatus = Shared.db().prepare("SELECT status FROM " + TABLE_REGISTRY + " WHERE path = ?;");
		}
		return _reqGetStatus;
	}
	private static PreparedStatement _reqSetStatus;

	private static PreparedStatement reqSetStatus() {
		if (_reqSetStatus == null) {
			_reqSetStatus = Shared.db().prepare("UPDATE " + TABLE_REGISTRY + " SET status = ? WHERE path = ?;");
		}
		return _reqSetStatus;
	}

	protected void setStatus(int value) {
		Shared.db().execute(reqSetStatus().bind(path, status));
		status = value;
	}
	Integer status;

	protected int getStatus() {
		if (status == null) {
			ResultSet rs = Shared.db().execute(reqGetStatus().bind(path));
			for (Row row : rs) {
				status = row.getInt(1);
			}
		}
		return status;
	}
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Properties management">
	private Map<String, String> values;
	private static PreparedStatement _reqGetValues;

	private static PreparedStatement reqGetValues() {
		if (_reqGetValues == null) {
			_reqGetValues = Shared.db().prepare("SELECT values FROM " + TABLE_REGISTRY + " WHERE path = ?;");
		}
		return _reqGetValues;
	}
	private static PreparedStatement _reqSaveValue;

	private static PreparedStatement reqSaveValue() {
		if (_reqSaveValue == null) {
			_reqSaveValue = Shared.db().prepare("UPDATE " + TABLE_REGISTRY + " SET values[ ? ] = ? WHERE path = ?;");
		}
		return _reqSaveValue;
	}

	public String getProperty(String name, String defaultValue) {
		String value = getValues().get(name);
		return value != null ? value : defaultValue;
	}

	public int getProperty(String name, int defaultValue) {
		String value = getProperty(name, (String) null);
		return value != null ? Integer.parseInt(value) : defaultValue;
	}

	public long getProperty(String name, long defaultValue) {
		String value = getProperty(name, (String) null);
		return value != null ? Long.parseLong(value) : defaultValue;
	}

	public Date getPropertyDate(String name) {
		long time = getProperty(name, (long) 0);
		return time != 0 ? new Date(time) : null;
	}

	public Map<String, String> getValues() {
		if (values == null) {
			ResultSet rs = Shared.db().execute(reqGetValues().bind(path));
			for (Row r : rs) {
				values = r.getMap(1, String.class, String.class);
				return values;
			}
			values = new HashMap<String, String>();
		}
		return values;
	}

	public void setProperty(String name, String value) {
		Shared.db().execute(reqSaveValue().bind(name, value, path));
		if (values != null) {
			values.put(name, value);
		}
	}

	public void setProperty(String name, long value) {
		setProperty(name, "" + value);
	}

	public void setProperty(String name, Date date) {
		setProperty(name, date.getTime());
	}
	// </editor-fold>
}
