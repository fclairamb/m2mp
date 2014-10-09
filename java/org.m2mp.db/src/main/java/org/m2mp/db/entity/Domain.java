package org.m2mp.db.entity;

import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Domain. Domain is like NT domains, or google apps domain. It helps keep a
 * group of users and/or devices together.
 * <p/>
 * This is mostly a code sample but it can be applied to pretty much all
 * projects.
 *
 * @author Florent Clairambault
 */
public class Domain extends Entity {

    private static final Pattern VALIDATION = Pattern.compile("^[a-z][a-z0-9\\-]{3,}$");
    private static final String NODE_DOMAIN = "/domain/",
            PROPERTY_MASTER_ID = "master",
            PROPERTY_NAME = "name",
            PROPERTY_CREATED_DATE = "created_date",
            NODE_DEVICES = "devices",
            NODE_USERS = "users",
            NODE_BY_NAME = NODE_DOMAIN + "by-name/";
    private UUID domainId;

    public Domain(UUID id) {
        domainId = id;
        node = new RegistryNode(NODE_DOMAIN + id);
    }

    public Domain(RegistryNode node) {
        domainId = UUID.fromString(node.getName());
        this.node = node;
    }

    public static Domain getDefault() {
        return Domain.byName("default", true);
    }

    public static Domain byName(String name, boolean create) {
        if (!VALIDATION.matcher(name).matches()) {
            throw new RuntimeException("Domain \"" + name + "\" doesn't match \"" + VALIDATION.pattern() + "\" matching pattern.");
        }
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

            if (domain.exists()) { // This can happen if a domain was created with the same name and then renamed
                domain = new Domain(UUID.randomUUID());
            }

            domain.create();
            domain.setProperty(PROPERTY_CREATED_DATE, new Date());
            domain.setName(name);
            return domain;
        } else {
            return null;
        }
    }

    public static Iterable<Domain> getAll() {
        return new Iterable<Domain>() {
            @Override
            public Iterator<Domain> iterator() {
                return new Iterator<Domain>() {

                    private final Iterator<String> iter;

                    private UUID next;

                    {
                        iter = new RegistryNode(NODE_DOMAIN).getChildrenNames().iterator();
                    }

                    @Override
                    public boolean hasNext() {
                        while (iter.hasNext()) {
                            String name = iter.next();
                            if (name.equals("by-name")) {
                                continue;
                            }
                            try {
                                next = UUID.fromString(name);
                            } catch (IllegalArgumentException ex) {
                                continue;
                            }
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public Domain next() {
                        return new Domain(next);
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
    }

    public String getName() {
        return getProperty(PROPERTY_NAME, null);
    }

    public void setName(String name) {
        if (name != null && byName(name, false) != null) {
            throw new IllegalArgumentException("This domain name is already taken !");
        }
        String previousName = getName();
        if (previousName != null) {
            new RegistryNode(NODE_BY_NAME + previousName).delete(true);
        }
        if (name != null) {
            new RegistryNode(NODE_BY_NAME + name).check().setProperty("id", getId());
            setProperty(PROPERTY_NAME, name);
        }
    }

    public UUID getId() {
        return domainId;
    }

    @Override
    public Domain check() {
        node.check();
        return this;
    }

    public User getMaster() {
        return getMaster();
    }

    public void setMaster(User master) {
        setProperty(PROPERTY_MASTER_ID, master.getId());
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

    private RegistryNode getDevicesNode() {
        return node.getChild(NODE_DEVICES).check();
    }

    public void addDevice(UUID deviceId) {
        getDevicesNode().setProperty(deviceId.toString(), "");
    }

    public void removeDevice(UUID deviceId) {
        getDevicesNode().delProperty(deviceId.toString());
    }

    public Iterable<Device> getDevices() {
        return new Iterable<Device>() {

            @Override
            public Iterator<Device> iterator() {
                return new Iterator<Device>() {

                    private final Iterator<String> iter;

                    private UUID next;

                    {
                        iter = getNode().getChild(NODE_DEVICES).getChildrenNames().iterator();
                    }

                    @Override
                    public boolean hasNext() {
                        while (iter.hasNext()) {
                            String name = iter.next();
                            if (name.equals("by-name")) {
                                continue;
                            }
                            try {
                                next = UUID.fromString(name);
                            } catch (IllegalArgumentException ex) {
                                continue;
                            }
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public Device next() {
                        return new Device(next);
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
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

    public Iterable<User> getUsers() {
        return new Iterable<User>() {

            @Override
            public Iterator<User> iterator() {
                return new Iterator<User>() {

                    private final Iterator<String> iter;

                    private UUID next;

                    {
                        iter = getNode().getChild(NODE_USERS).getChildrenNames().iterator();
                    }

                    @Override
                    public boolean hasNext() {
                        while (iter.hasNext()) {
                            String name = iter.next();
                            if (name.equals("by-name")) {
                                continue;
                            }
                            try {
                                next = UUID.fromString(name);
                            } catch (IllegalArgumentException ex) {
                                continue;
                            }
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public User next() {
                        return new User(next);
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
    }

    @Override
    public void delete() {
        setName(null);
        node.delete(true);
    }

    /**
     * Timeserie ID
     *
     * @return Timeserie ID
     */
    public String getTSId() {
        return "dom-" + getId();
    }
}
