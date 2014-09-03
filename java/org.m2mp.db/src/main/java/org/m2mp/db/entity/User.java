package org.m2mp.db.entity;

import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;

/**
 * User data container.
 * <p/>
 * This is mostly a code sample but it can be applied to pretty much all
 * projects.
 *
 * @author Florent Clairambault
 */
public class User extends Entity {

    private static final String NODE_USER = "/user/";
    private static final String NODE_BY_NAME = NODE_USER + "by-name/";

    public User(UUID userId) {
        node = new RegistryNode(NODE_USER + userId);
    }

    public User(RegistryNode node) {
        this.node = node;
    }

    private UUID id;
    public UUID getId() {
        if (id == null)
            id = UUID.fromString(node.getName());
        return id;
    }

    public static User byName(String name, boolean create) {
        RegistryNode node = new RegistryNode(NODE_BY_NAME + name);
        if (node.exists()) {
            return new User(node.getPropertyUUID("id"));
        } else if (create) {
            byte[] digest = new byte[20];
            try {
                digest = MessageDigest.getInstance("SHA-1").digest(name.getBytes());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            byte[] uuidRaw = new byte[16];
            System.arraycopy(digest, 0, uuidRaw, 0, 16);
            UUID userId = UUID.nameUUIDFromBytes(uuidRaw);
            User user = new User(userId);

            if (user.exists()) {
                user = new User(UUID.randomUUID());
            } else {
                user.create();
            }
            user.setProperty(PROP_CREATED_DATE, new Date());
            user.setName(name);
            return user;
        } else {
            return null;
        }
    }

    private static final String PROP_NAME = "name";
    private static final String PROP_DISPLAYNAME = "displayName";
    private static final String PROP_DOMAIN = "domain";
    private static final String PROP_CREATED_DATE = "created";
    private static final String PROP_PASSWORD = "password";

    /**
     * Set the username
     *
     * @param name Username
     */
    public void setName(String name) {
        String previousName = getProperty(PROP_NAME, null);
        if (previousName != null) {
            new RegistryNode(NODE_BY_NAME + previousName).delete(true);
        }
        setProperty(PROP_NAME, name);
        new RegistryNode(NODE_BY_NAME + name).check().setProperty("id", getId());
    }

    public String getName() {
        return getProperty(PROP_NAME, null);
    }

    public String getPassword() {
        return getProperty(PROP_PASSWORD, null);
    }

    public static User authenticate(String username, String password) {
        User user = User.byName(username, false);
        return (user != null && password != null && password.equals(user.getPassword())) ? user : null;
    }

    public void setPassword(String pass) {
        setProperty(PROP_PASSWORD, pass);
    }

    public UUID getDomainId() {
        return getPropertyUUID(PROP_DOMAIN);
    }

    public Domain getDomain() {
        return new Domain(getDomainId());
    }

    public void setDomain(Domain domain) {
        Domain previousDomain = getDomain();
        if (previousDomain != null) {
            previousDomain.removeUser(getId());
        }
        setProperty(PROP_DOMAIN, domain.getId());
        getDomain().addUser(getId());
    }

    @Override
    protected int getObjectVersion() {
        return 1;
    }

    private RegistryNode settings;

    public RegistryNode getSettings() {
        if (settings == null) {
            settings = node.getChild("settings");
        }
        return settings;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof User) {
            User ou = (User) obj;
            return ou.getId().equals(getId());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    public User check() {
        super.check();
        return this;
    }
}
