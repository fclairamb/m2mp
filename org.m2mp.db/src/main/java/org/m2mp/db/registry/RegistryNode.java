package org.m2mp.db.registry;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.m2mp.db.DBAccess;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 *
 * The registry is organized to allow fast access to any node by its name. The
 * trade-off is that we can't easily move nodes. Move can only be implemented by
 * copy (thus should be avoid avoid).
 */
public class RegistryNode {

	protected DBAccess db;
	protected String path;

	public RegistryNode(DBAccess db, String path) {
		if (!path.endsWith("/")) {
			path += "/";
		}
		this.db = db;
		this.path = path;
	}

	public DBAccess getDb() {
		return db;
	}

	public String getPath() {
		return path;
	}

	public RegistryNode check() {
		switch (getStatus()) {
			case STATUS_UNDEFINED:
			case STATUS_DELETED:
				create();
		}
		return this;
	}

	public void create() {
		setStatus(STATUS_CREATED);
		RegistryNode parent = getParentNode();
		if (parent != null) {
			parent.check();
			parent.addChild(getName());
		}
	}

	public void delete() {
		setStatus(STATUS_DELETED);
		for (RegistryNode child : getChildren()) {
			child.delete();
		}
		RegistryNode parent = getParentNode();
		if (parent != null) {
			parent.removeChild(getName());
		}
	}

	public boolean exists() {
		return getStatus() == STATUS_CREATED;
	}

	public boolean existed() {
		return getStatus() != STATUS_UNDEFINED;
	}

	public RegistryNode getParentNode() {
		String parentPath = convPathToBase(path);
		if (parentPath == null) {
			return null;
		}
		return new RegistryNode(db, parentPath);
	}

	private static String convPathToBase(String path) {
		int p = path.lastIndexOf('/');

		// The "/" dir doesn't have a parent
		if (p == 0) {
			return null;
		}

		if (path.endsWith("/")) {
			p = path.substring(0, p).lastIndexOf('/');
		}
		return path.substring(0, p + 1);
	}

	private static String convPathToName(String path) {
		int p = path.lastIndexOf('/');

		if (path.endsWith("/")) {
			p = path.substring(0, p).lastIndexOf('/');
		}

		String name = path.substring(p + 1);

		if (name.endsWith("/")) {
			name = name.substring(0, name.length() - 1);
		}

		return name;
	}

	public String getName() {
		return convPathToName(path);
	}
	// <editor-fold defaultstate="collapsed" desc="Column family preparation">
	public static final String TABLE_REGISTRY = "RegistryNode";

	public static void prepareTable(DBAccess db) {
		GeneralSetting.prepareTable(db);
		TableCreation.checkTable(db, new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE_REGISTRY;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_REGISTRY + " ( path text PRIMARY KEY, values map<text,text>, status int );"));
				list.add(new TableIncrementalDefinition.TableChange(2, "CREATE TABLE " + TABLE_REGISTRY + "Children ( path text, name text, PRIMARY KEY( path, name ) ) WITH CLUSTERING ORDER BY ( name ASC );"));
				return list;
			}

			@Override
			public int getTableDefVersion() {
				return 1;
			}
		});
	}
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Children management">
	private PreparedStatement _reqInsertChildName;

	private PreparedStatement reqInsertChildName() {
		if (_reqInsertChildName == null) {
			_reqInsertChildName = db.prepare("INSERT INTO " + TABLE_REGISTRY + "Children ( path, name ) VALUES ( ?, ? );");
		}
		return _reqInsertChildName;
	}
	private PreparedStatement _reqDeleteChildName;

	private PreparedStatement reqDeleteChildName() {
		if (_reqDeleteChildName == null) {
			_reqDeleteChildName = db.prepare("DELETE FROM " + TABLE_REGISTRY + "Children WHERE path = ? AND name = ?;");
		}
		return _reqDeleteChildName;
	}
	private PreparedStatement _reqListChildren;

	private PreparedStatement reqListChildrenNames() {
		if (_reqListChildren == null) {
			_reqListChildren = db.prepare("SELECT name FROM " + TABLE_REGISTRY + "Children WHERE path = ?;");
		}
		return _reqListChildren;
	}

	private void addChild(String name) {
		db.execute(reqInsertChildName().bind(path, name));
	}

	private void removeChild(String name) {
		db.execute(reqDeleteChildName().bind(path, name));
	}

	private Iterable<String> getChildrenNames() {
		final Iterator<Row> iter = db.execute(reqListChildrenNames().bind(path)).iterator();
		return new Iterable<String>() {
			@Override
			public Iterator<String> iterator() {
				return new Iterator<String>() {
					@Override
					public boolean hasNext() {
						return iter.hasNext();
					}

					@Override
					public String next() {
						return iter.next().getString(0);
					}

					@Override
					public void remove() {
					}
				};
			}
		};
	}

	public Iterable<RegistryNode> getChildren() {
		final Iterator<String> iter = getChildrenNames().iterator();
		return new Iterable<RegistryNode>() {
			@Override
			public Iterator<RegistryNode> iterator() {
				return new Iterator<RegistryNode>() {
					@Override
					public boolean hasNext() {
						return iter.hasNext();
					}

					@Override
					public RegistryNode next() {
						return new RegistryNode(db, path + iter.next() + "/");
					}

					@Override
					public void remove() {
					}
				};
			}
		};
	}

	/**
	 * Get the number of children.
	 *
	 * @param withSubs Add the sub-children
	 * @return Number of children.
	 *
	 * This method is *NOT* optimized at all. It's built for testing purpose
	 * only at this stage purpose.
	 */
	public int getNbChildren(boolean withSubs) {
		int nb = 0;
		for (RegistryNode child : getChildren()) {
			nb += 1 + (withSubs ? child.getNbChildren(true) : 0);
		}
		return nb;
	}
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Status management">
	private static final int STATUS_UNDEFINED = -1;
	private static final int STATUS_DELETED = 5;
	private static final int STATUS_CREATED = 100;
	private PreparedStatement reqGetStatus;

	private PreparedStatement reqGetStatus() {
		if (reqGetStatus == null) {
			reqGetStatus = db.prepare("SELECT status FROM " + TABLE_REGISTRY + " WHERE path = ?;");
		}
		return reqGetStatus;
	}
	private PreparedStatement _reqSetStatus;

	private PreparedStatement reqSetStatus() {
		if (_reqSetStatus == null) {
			_reqSetStatus = db.prepare("UPDATE " + TABLE_REGISTRY + " SET status = ? WHERE path = ?;");
		}
		return _reqSetStatus;
	}

	protected void setStatus(int value) {
		db.execute(reqSetStatus().bind(value, path));
		status = value;
	}
	Integer status;

	protected int getStatus() {
		if (status == null) {
			ResultSet rs = db.execute(reqGetStatus().bind(path));
			for (Row row : rs) {
				status = row.getInt(0);
			}
		}
		return status != null ? status : STATUS_UNDEFINED;
	}
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Properties management">
	private Map<String, String> values;
	private PreparedStatement reqGetValues;

	private PreparedStatement reqGetValues() {
		if (reqGetValues == null) {
			reqGetValues = db.prepare("SELECT values FROM " + TABLE_REGISTRY + " WHERE path = ?;");
		}
		return reqGetValues;
	}
	private PreparedStatement reqSaveValue;

	private PreparedStatement reqSaveValue() {
		if (reqSaveValue == null) {
			reqSaveValue = db.prepare("UPDATE " + TABLE_REGISTRY + " SET values[ ? ] = ? WHERE path = ?;");
		}
		return reqSaveValue;
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
			ResultSet rs = db.execute(reqGetValues().bind(path));
			for (Row r : rs) {
				values = new HashMap<>(r.getMap(0, String.class, String.class));
				return values;
			}
			values = new HashMap<>();
		}
		return values;
	}

	public void setProperty(String name, String value) {
		db.execute(reqSaveValue().bind(name, value, path));
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

	@Override
	public String toString() {
		return path;
	}
}
