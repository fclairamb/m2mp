package org.m2mp.db.registry;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.m2mp.db.DB;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.file.DbFile;

import java.util.*;

/**
 * Registry node.
 *
 * @author Florent Clairambault
 *         <p/>
 *         The registry is organized to allow fast access to any node by its name. The
 *         trade-off is that we can't easily move nodes. Move can only be implemented by
 *         copy (thus should be avoid avoid).
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
     * Check if it's a file.
     *
     * @return If it's a file
     */
    public boolean isFile() {
        return getProperty(RegistryNode.PROPERTY_IS_FILE, false);
    }

    /**
     * Delete the node.
     * <p/>
     * If forReal is not applied, data is just *marked* as deleted. Which means
     * it's still very present but not connected to its parent
     *
     * @param forReal To actually do it
     */
    public void delete(boolean forReal) {
        for (RegistryNode child : getChildren()) {
            child.delete(forReal);
        }
        if (forReal) {
            DB.execute(DB.prepare("DELETE FROM " + TABLE_REGISTRY + " WHERE path=?;").bind(path));
            DB.execute(DB.prepare("DELETE FROM " + TABLE_REGISTRY + "Data WHERE path=?;").bind(path));
            properties = null;
        } else {
            // We're just marking this as requiring a deletion
            setStatus(STATUS_DELETED);
        }
        RegistryNode parent = getParentNode();
        if (parent != null) {
            parent.removeChild(getName());
        }
    }

    /**
     * Cleanup all the old values.
     */
    public static void cleanup() {
        for (Row row : DB.execute(DB.prepare("SELECT path FROM " + TABLE_REGISTRY + " WHERE status=?;").bind(STATUS_DELETED))) {
            RegistryNode node = new RegistryNode(row.getString(0));
            node.delete(true);
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

    public boolean deleted() {
        return getStatus() == STATUS_DELETED;
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
     * @return Base path to getData
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
     * <p/>
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
                list.add(new TableIncrementalDefinition.TableChange(3, "CREATE INDEX ON " + TABLE_REGISTRY + " (status);"));
                return list;
            }

            @Override
            public int getTableDefVersion() {
                return 3;
            }
        });
        DbFile.prepareTable();
    }

    public static void dropTable() {
        final String[] queries = new String[]{
                "DROP TABLE registrynode;",
                "DROP TABLE registrynodechildren",
                "DROP TABLE registrynodedata"
        };
        for (String query : queries) {
            try {
                DB.execute(query);
            } catch (Exception ex) {

            }
        }
    }
    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Children management">

    private void addChild(String name) {
        DB.execute(DB.prepare("INSERT INTO " + TABLE_REGISTRY + "Children ( path, name ) VALUES ( ?, ? );").bind(path, name));
    }

    private void removeChild(String name) {
        DB.execute(DB.prepare("DELETE FROM " + TABLE_REGISTRY + "Children WHERE path = ? AND name = ?;").bind(path, name));
    }

    public Iterable<String> getChildrenNames() {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {

                final Iterator<Row> iter = DB.execute(DB.prepare("SELECT name FROM " + TABLE_REGISTRY + "Children WHERE path = ?;").bind(path)).iterator();

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

    public Iterable<String> getChildrenUndottedNames() {

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    final Iterator<String> iter = getChildrenNames().iterator();
                    private String next;

                    @Override
                    public boolean hasNext() {
                        while (iter.hasNext()) {
                            next = iter.next();

                            // We skip all names starting with "."
                            if (next.startsWith(".")) {
                                continue;
                            }

                            return true;
                        }
                        return false;
                    }

                    @Override
                    public String next() {
                        return next;
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
     * <p/>
     * There's no limit on the number of children nodes that we can read.
     *
     * @param includingHidden Include hidden children
     * @return the children nodes
     */
    public Iterable<RegistryNode> getChildren(final boolean includingHidden) {
        return new Iterable<RegistryNode>() {
            @Override
            public Iterator<RegistryNode> iterator() {
                return new Iterator<RegistryNode>() {
                    final Iterator<String> iter = (includingHidden ? getChildrenNames() : getChildrenUndottedNames()).iterator();

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
     * Get children nodes.
     * <br />
     * This returns all the children (including hidden ones).
     *
     * @return all the children nodes
     */
    public Iterable<RegistryNode> getChildren() {
        return getChildren(true);
    }

    /**
     * Get one child registry node.
     *
     * @param name Name of the registry node child
     * @return A new RegistryNode instance
     */
    public RegistryNode getChild(String name) {
        return new RegistryNode(path + name + "/");
    }

    /**
     * Get the number of children.
     *
     * @param withSubs Add the sub-children
     * @return Number of children.
     * <p>
     * This method is *NOT* optimized at all.
     * </p>
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
    private Map<String, String> properties;
    public static final String PROPERTY_IS_FILE = ".is_file";

    /**
     * Get a property
     *
     * @param name         Name of the property
     * @param defaultValue Default value of the property
     * @return Value of the property
     */
    public String getProperty(String name, String defaultValue) {
        String value = getProperties().get(name);
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

    public boolean getProperty(String name, boolean defaultValue) {
        return getProperty(name, defaultValue ? "1" : "0").equals("1");
    }

    public String getPropertyString(String name) {
        return getProperty(name, (String) null);
    }

    public Date getPropertyDate(String name) {
        long time = getProperty(name, (long) 0);
        return time != 0 ? new Date(time) : null;
    }

    public UUID getPropertyUUID(String name) {
        String id = getProperty(name, (String) null);
        return id != null ? UUID.fromString(id) : null;
    }

    protected void setProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> prop : properties.entrySet()) {
            setProperty(prop.getKey(), prop.getValue());
        }
    }

    public Map<String, String> getProperties() {
        if (properties == null) {
            ResultSet rs = DB.execute(DB.prepare("SELECT values FROM " + TABLE_REGISTRY + " WHERE path = ?;").bind(path));
            for (Row r : rs) {
                properties = new HashMap<>(r.getMap(0, String.class, String.class));
                return properties;
            }
            properties = new HashMap<>();
        }
        return properties;
    }

    public void delProperty(String name) {
        DB.execute(DB.prepare("DELETE values[ ? ] FROM " + TABLE_REGISTRY + " WHERE path = ?;").bind(name, path));
        if (properties != null) {
            properties.remove(name);
        }
    }

    public void setProperty(String name, String value) {
        DB.execute(DB.prepare("UPDATE " + TABLE_REGISTRY + " SET values[ ? ] = ? WHERE path = ?;").bind(name, value, path));
        if (properties != null) {
            properties.put(name, value);
        }
    }

    public void setProperty(String name, String value, int ttl) {
        DB.execute(DB.prepare("UPDATE " + TABLE_REGISTRY + " USING TTL ? SET values[ ? ] = ? WHERE path = ?;").bind(ttl, name, value, path));
        if (properties != null) {
            properties.put(name, value);
        }
    }

    public void setProperty(String name, boolean value) {
        setProperty(name, value ? "1" : "0");
    }

    public void setProperty(String name, long value) {
        setProperty(name, "" + value);
    }

    public void setProperty(String name, long value, int ttl) {
        setProperty(name, "" + value, ttl);
    }

    public void setProperty(String name, Date date) {
        setProperty(name, date.getTime());
    }

    public void setProperty(String name, UUID id) {
        setProperty(name, id.toString());
    }
    // </editor-fold>

    @Override
    public String toString() {
        return path;
    }

    /**
     * Convert to JSON object.
     * <p/>
     * This convert any node to a json object (with only string values). This
     * helps you serialize any kind of information you were previously storing
     * as a node.
     * <p/>
     * A good example a special command you will send to a device. You want to
     * store it and send it in JSON to the device "as-is".
     *
     * @return JSON object
     */
    public JSONObject toJsonObject() {
        JSONObject obj = new JSONObject();

        for (Map.Entry<String, String> me : getProperties().entrySet()) {
            obj.put(me.getKey(), JSONValue.parse(me.getValue()));
        }

        for (RegistryNode rn : getChildren()) {
            obj.put(rn.getName(), rn.toJsonObject());
        }

        return obj;
    }

    /**
     * Copy a registry node to an other node.
     *
     * @param dst Destination to copy to
     * @return if it worked
     */
    public boolean copyTo(RegistryNode dst) {
        dst.check().setProperties(getProperties());
        for (RegistryNode child : getChildren()) {
            child.copyTo(dst.getChild(child.getName()));
        }
        return true;
    }

    public boolean moveTo(RegistryNode dst, boolean forReal) {
        if (copyTo(dst)) {
            delete(forReal);
            return true;
        }
        return false;
    }

    public String toJsonString() {
        return toJsonObject().toJSONString();
    }

    void loadJsonObject(JSONObject obj) {
        if (obj == null) {
            return;
        }

        Iterator iter = obj.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            Object value = entry.getValue();
            if (value instanceof JSONObject) {
                getChild(key).check().loadJsonObject((JSONObject) value);
            } else {
                // Everything (an integer or an array for example) becomes strings in here. I never said it was a JSON-compatible storage.
                setProperty(key, value.toString());
            }
        }
    }

    public Iterable<String> getPropertyNames() {
        return getProperties().keySet();
    }

    @Deprecated
    public String getValueString(String package_name) {
        return getProperty(package_name, null);
    }

    @Deprecated
    public void setValue(String pkey, String s) {
        setProperty(pkey, s);
    }

    @Deprecated
    public void setValue(String name, UUID value) {
        setProperty(name, value);
    }

    @Deprecated
    public void setValue(String symbolName, Date date) {
        setProperty(symbolName, date);
    }

    @Deprecated
    public Map<String, String> getMap() {
        return getProperties();
    }

    @Deprecated
    public Date getValueDate(String symbolName) {
        return getPropertyDate(symbolName);
    }

    @Deprecated
    public void delValue(String symbolName) {
        delProperty(symbolName);
    }

    @Deprecated
    public Iterable<String> getKeysList() {
        return getPropertyNames();
    }

    @Deprecated
    public String getValueString(String name, String defaultValue) {
        return getProperty(name, defaultValue);
    }

    @Deprecated
    public long getValueLong(String name, long defaultValue) {
        return getProperty(name, defaultValue);
    }

    @Deprecated
    public int getValueInt(String propertyName, int defaultValue) {
        return getProperty(propertyName, defaultValue);
    }

    @Deprecated
    public Collection<String> getValuesList() {
        return getProperties().values();
    }

    @Deprecated
    public UUID getValueUUID(String name) {
        return getPropertyUUID(name);
    }

    @Deprecated
    public String getValue(String name) {
        return getPropertyString(name);
    }
}
