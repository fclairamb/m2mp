package org.m2mp.db.entity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.m2mp.db.DB;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Domain. Domain is like NT domains, or google apps domain. It helps keep a
 * group of users and/or devices together.
 * <p/>
 * This is mostly a code sample but it can be applied to pretty much all projects.
 *
 * @author Florent Clairambault
 */
public class Domain extends Entity {

    public static Domain getDefault() {
        return new Domain("__default__").check();
    }

    private UUID domainId;
    private static final String PREFIX = "/domain/";
    private static final String PROP_MASTER_ID = "master";

    public Domain(UUID id) {
        domainId = id;
        node = new RegistryNode(PREFIX + id);
    }

    public Domain(RegistryNode node) {
        domainId = UUID.fromString(node.getName());
        this.node = node;
    }

    public Domain(String name) {
        domainId = getIdFromName(name);
        if (domainId != null) {
            node = new RegistryNode(PREFIX + domainId);
        } else {
            throw new IllegalArgumentException("The domain \"" + name + "\" could not be found !");
        }
    }

    public static Domain get(String name) {
        UUID domainId = getIdFromName(name);
        return domainId != null ? new Domain(domainId) : null;
    }

    private static final String PROP_NAME = "name";
    private static final String PROP_CREATED_DATE = "created";

    public static Domain create(String name) {
        UUID domainId = getIdFromName(name);
        if (domainId != null) {
            throw new IllegalArgumentException("The domain \"" + name + "\" already exists for domain \"" + domainId + "\"");
        }
        domainId = UUID.randomUUID();
        Domain d = new Domain(domainId);
        d.check();
        d.setName(name);
        d.setProperty(PROP_CREATED_DATE, System.currentTimeMillis());
        return d;
    }

    public String getName() {
        return getProperty(PROP_NAME, null);
    }

    public void setName(String name) {
        if (getIdFromName(name) != null)
            throw new IllegalArgumentException("This domain name is already taken !");
        String previousName = getName();
        setProperty(PROP_NAME, name);
        DB.execute(DB.prepare("INSERT INTO " + TABLE + " ( name, id ) VALUES ( ?, ? );").bind(name, domainId));
        if (previousName != null)
            DB.execute(DB.prepare("DELETE FROM " + TABLE + " WHERE name = ?;").bind(previousName));
    }

    public static final String TABLE = "Domain";

    protected static UUID getIdFromName(String name) {
        ResultSet rs = DB.execute(DB.prepare("SELECT id FROM " + TABLE + " WHERE name = ?;").bind(name));
        for (Row row : rs) {
            return row.getUUID(0);
        }
        return null;
    }

    public UUID getId() {
        return domainId;
    }

    public static void prepareTable() {
        RegistryNode.prepareTable();
        TableCreation.checkTable(new TableIncrementalDefinition() {
            @Override
            public String getTableDefName() {
                return TABLE;
            }

            @Override
            public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
                List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
                list.add(new TableIncrementalDefinition.TableChange(1, ""
                        + "CREATE TABLE " + TABLE + " (\n"
                        + "  name text PRIMARY KEY,"
                        + "  id uuid\n"
                        + ");"));
                return list;
            }

            public String getTablesDefCql() {
                return ""
                        + "CREATE TABLE " + TABLE + " (\n"
                        + "  name text PRIMARY KEY,"
                        + "  id uuid\n"
                        + ");";
            }

            @Override
            public int getTableDefVersion() {
                return 1;
            }
        });
    }

    @Override
    public Domain check() {
        node.check();
//		if (!exists()) {
//			create(getName());
//		}
        return this;
    }

    @Deprecated
    public User getDefaultUser() {
        return User.get("__" + getName() + "__");
    }

    public void setMaster(User master) {
        setProperty(PROP_MASTER_ID, master.getId());
    }

    public User getMaster() {
        return getMaster();
    }

    @Override
    protected int getObjectVersion() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj instanceof Domain && getId().equals(((Domain) obj).getId());
    }

    @Override
    public int hashCode() {
        return domainId.hashCode();
    }

    public Iterable<Domain> getAll() {
        return new Iterable<Domain>() {

            @Override
            public Iterator<Domain> iterator() {
                return new Iterator<Domain>() {


                    @Override
                    public boolean hasNext() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public Domain next() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }
                };
            }

            ;
        };
    }
}