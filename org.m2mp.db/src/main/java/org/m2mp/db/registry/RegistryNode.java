package org.m2mp.db.registry;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.m2mp.db.DB;
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

	/**
	 * Path of the node
	 */
	protected String path;

	/**
	 * Constructor
	 *
	 * @param path Path of the node
	 */
	public RegistryNode(String path) {
		if (!path.endsWith("/")) {
			path += "/";
		}
		this.path = path;
	}

	/**
	 * Get the path
	 *
	 * @return Path of the node
	 */
	public String getPath() {
		return path;
	}

	/**
	 * Check if the node does exists and if not, create it.
	 *
	 * @return Checked node
	 */
	public RegistryNode check() {
		switch (getStatus()) {
			case STATUS_UNDEFINED:
			case STATUS_DELETED:
				create();
		}
		return this;
	}

	/**
	 * Create the node
	 */
	public RegistryNode create() {
		setStatus(STATUS_CREATED);
		RegistryNode parent = getParentNode();
		if (parent != null) {
			parent.check();
			parent.addChild(getName());
		}
		return this;
	}

	/**
	 * Delete the node
	 */
	public void delete() {
		delete(false);
	}

	/**
	 * Delete the node.
	 *
	 * @param forReal Delete its content and children contents
	 */
	public void delete(boolean forReal) {
		for (RegistryNode child : getChildren()) {
			child.delete(forReal);
		}
		if (forReal) {
			DB.execute(DB.prepare("DELETE values, status FROM " + TABLE_REGISTRY + " WHERE path=?;").bind(path));
			values = null;
		} else {
			setStatus(STATUS_DELETED);
			// We don't touch the values because they haven't been deleted
		}
		RegistryNode parent = getParentNode();
		if (parent != null) {
			parent.removeChild(getName());
		}
	}

	/**
	 * Check if a node exists.
	 *
	 * @return If the node exists
	 */
	public boolean exists() {
		return getStatus() == STATUS_CREATED;
	}

	/**
	 * Check if a node exists or existed (was only marked as deleted).
	 *
	 * @return If the node existed
	 */
	public boolean existed() {
		return getStatus() != STATUS_UNDEFINED;
	}

	/**
	 * Get the parent node.
	 *
	 * @return Parent node
	 */
	public RegistryNode getParentNode() {
		String parentPath = convPathToBase(path);
		if (parentPath == null) {
			return null;
		}
		return new RegistryNode(parentPath);
	}

	/**
	 * Get the base path from a path
	 *
	 * @param path Path to check
	 * @return Base path to get
	 */
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

	/**
	 * Get the name of the node.
	 *
	 * Example: new RegistryNode("/path/of/my_file").getName() == "my_file"
	 *
	 * @return Name of the node
	 */
	public String getName() {
		return convPathToName(path);
	}
	// <editor-fold defaultstate="collapsed" desc="Column family preparation">
	public static final String TABLE_REGISTRY = "RegistryNode";

	public static void prepareTable() {
		GeneralSetting.prepareTable();
		TableCreation.checkTable(new TableIncrementalDefinition() {
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

	private void addChild(String name) {
		DB.execute(DB.prepare("INSERT INTO " + TABLE_REGISTRY + "Children ( path, name ) VALUES ( ?, ? );").bind(path, name));
	}

	private void removeChild(String name) {
		DB.execute(DB.prepare("DELETE FROM " + TABLE_REGISTRY + "Children WHERE path = ? AND name = ?;").bind(path, name));
	}

	private Iterable<String> getChildrenNames() {
		final Iterator<Row> iter = DB.execute(DB.prepare("SELECT name FROM " + TABLE_REGISTRY + "Children WHERE path = ?;").bind(path)).iterator();
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

	/**
	 * Get children nodes.
	 *
	 * There's no limit on the number of children nodes that we can read.
	 *
	 * @return all the children nodes
	 */
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
						return new RegistryNode(path + iter.next() + "/");
					}

					@Override
					public void remove() {
					}
				};
			}
		};
	}

	/**
	 * Get one child registry node.
	 *
	 * @param name Name of the registry node child
	 * @return
	 */
	public RegistryNode getChild(String name) {
		return new RegistryNode(path + name + "/");
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

	protected void setStatus(int value) {
		DB.execute(DB.prepare("UPDATE " + TABLE_REGISTRY + " SET status = ? WHERE path = ?;").bind(value, path));
		status = value;
	}
	Integer status;

	protected int getStatus() {
		if (status == null) {
			ResultSet rs = DB.execute(DB.prepare("SELECT status FROM " + TABLE_REGISTRY + " WHERE path = ?;").bind(path));
			for (Row row : rs) {
				status = row.getInt(0);
			}
		}
		return status != null ? status : STATUS_UNDEFINED;
	}
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Properties management">
	private Map<String, String> values;

	/**
	 * Get a property
	 * @param name Name of the property
	 * @param defaultValue Default value of the property
	 * @return Value of the property
	 */
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
			ResultSet rs = DB.execute(DB.prepare("SELECT values FROM " + TABLE_REGISTRY + " WHERE path = ?;").bind(path));
			for (Row r : rs) {
				values = new HashMap<>(r.getMap(0, String.class, String.class));
				return values;
			}
			values = new HashMap<>();
		}
		return values;
	}

	public void delProperty(String name) {
		DB.execute(DB.prepare("DELETE values[ ? ] FROM " + TABLE_REGISTRY + " WHERE path = ?;").bind(name, path));
		if (values != null) {
			values.remove(name);
		}
	}

	public void setProperty(String name, String value) {
		DB.execute(DB.prepare("UPDATE " + TABLE_REGISTRY + " SET values[ ? ] = ? WHERE path = ?;").bind(name, value, path));
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
