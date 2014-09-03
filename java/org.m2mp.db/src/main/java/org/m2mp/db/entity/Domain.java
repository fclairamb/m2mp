package org.m2mp.db.entity;

import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
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
        return Domain.byName("default", true);
    }

    private UUID domainId;
    private static final String NODE_DOMAIN = "/domain/",
            PROP_MASTER_ID = "master",
            PROP_NAME = "name",
            PROP_CREATED_DATE = "created_date",
            NODE_DEVICES = "devices",
            NODE_USERS = "users",
            NODE_BY_NAME = NODE_DOMAIN + "by-name/";

    public Domain(UUID id) {
        domainId = id;
        node = new RegistryNode(NODE_DOMAIN + id);
    }

    public Domain(RegistryNode node) {
        domainId = UUID.fromString(node.getName());
        this.node = node;
    }

    public static Domain byName(String name, boolean create) {
        RegistryNode node = new RegistryNode(NODE_BY_NAME + name);
        if (node.exists()) {
            return new Domain(node.getPropertyUUID("id"));
        } else if (create) {
            byte[] digest = new byte[20];
            try {
                digest = MessageDigest.getInstance("SHA-1").digest(name.getBytes());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            byte[] uuidRaw = new byte[16];
            System.arraycopy(digest, 0, uuidRaw, 0, 16);
            UUID deviceId = UUID.nameUUIDFromBytes(uuidRaw);
            Domain domain = new Domain(deviceId);

            if (domain.exists()) {
                domain = new Domain(UUID.randomUUID());
            } else {
                domain.create();
            }
            domain.setProperty(PROP_CREATED_DATE, new Date());
            domain.setName(name);
            return domain;
        } else {
            return null;
        }
    }

    public String getName() {
        return getProperty(PROP_NAME, null);
    }

    public void setName(String name) {
        if (byName(name, false) != null)
            throw new IllegalArgumentException("This domain name is already taken !");
        String previousName = getName();
        if (previousName != null) {
            new RegistryNode(NODE_BY_NAME + previousName).delete(true);
        }
        if (name != null) {
            new RegistryNode(NODE_BY_NAME + name).check().setProperty("id", getId());
            setProperty(PROP_NAME, name);
        }
    }

    public static final String TABLE = "Domain";

    public UUID getId() {
        return domainId;
    }

    @Override
    public Domain check() {
        node.check();
        return this;
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

    private RegistryNode getDevicesNode() {
        return node.getChild(NODE_DEVICES).check();
    }

    public void addDevice(UUID deviceId) {
        getDevicesNode().setProperty(deviceId.toString(), "");
    }

    public void removeDevice(UUID deviceId) {
        getDevicesNode().delProperty(deviceId.toString());
    }

    private RegistryNode getUsersNode() {
        return node.getChild(NODE_USERS).check();
    }

    public void addUser(UUID userId) {
        getUsersNode().setProperty(userId.toString(), "");
    }

    public void removeUser(UUID userId) {
        getUsersNode().delProperty(userId.toString());
    }

    public void delete() {
        setName(null);
        node.delete(false);
    }
}
