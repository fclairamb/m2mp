package org.m2mp.db.entity;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.m2mp.db.common.Entity;
import org.m2mp.db.registry.RegistryNode;

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
	private static final String NODE_SETTINGS = "settings";

	private static final Pattern VALIDATION = Pattern.compile("^[a-z][a-z0-9]{3,24}$");

	public User(UUID userId) {
		node = new RegistryNode(NODE_USER + userId);
	}

	public User(RegistryNode node) {
		this.node = node;
	}

	private UUID id;

	public UUID getId() {
		if (id == null) {
			id = UUID.fromString(node.getName());
		}
		return id;
	}

	public static User byName(String name, boolean create) {
		RegistryNode node = new RegistryNode(NODE_BY_NAME + name);
		if (node.exists()) {
			return new User(node.getPropertyUUID("id"));
		} else if (create) {
			if (!VALIDATION.matcher(name).matches()) {
				throw new RuntimeException("User \"" + name + "\" doesn't match the \"" + VALIDATION.pattern() + "\" pattern.");
			}
			byte[] digest = new byte[20];
			try {
				digest = MessageDigest.getInstance("SHA-1").digest(name.getBytes());
			} catch (NoSuchAlgorithmException ex) {
				Logger.getLogger(User.class.getName()).log(Level.SEVERE, null, ex);
			}
			byte[] uuidRaw = new byte[16];
			System.arraycopy(digest, 0, uuidRaw, 0, 16);
			UUID userId = UUID.nameUUIDFromBytes(uuidRaw);
			User user = new User(userId);

			if (user.exists()) {
				user = new User(UUID.randomUUID());
			}

			user.create();
			user.setProperty(PROPERTY_CREATED_DATE, new Date());
			user.setName(name);
			return user;
		} else {
			return null;
		}
	}

	public static User byName(String name) {
		return byName(name, false);
	}

	private static final String PROPERTY_NAME = "name";
	private static final String PROPERTY_DISPLAYNAME = "display_name";
	private static final String PROPERTY_DOMAIN = "domain";
	private static final String PROPERTY_CREATED_DATE = "created_date";
	private static final String PROPERTY_PASSWORD = "password";

	/**
	 * Set the username
	 *
	 * @param name Username
	 */
	public void setName(String name) {
		String previousName = getProperty(PROPERTY_NAME, null);
		if (previousName != null) {
			new RegistryNode(NODE_BY_NAME + previousName).delete(true);
		}
		setProperty(PROPERTY_NAME, name);
		new RegistryNode(NODE_BY_NAME + name).check().setProperty("id", getId());
	}

	public String getName() {
		return getProperty(PROPERTY_NAME, null);
	}

	public String getDisplayName() {
		return getProperty(PROPERTY_DISPLAYNAME, "");
	}

	public void setDisplayName(String value) {
		setProperty(PROPERTY_DISPLAYNAME, value);
	}

	public String getPassword() {
		return getProperty(PROPERTY_PASSWORD, null);
	}

	/**
	 * Set the password.
	 *
	 * There is no password mechanism specified here. It has to be handled on
	 * the upper levels. It is wise to choose something like "[salt];[sha1 of
	 * password + salt]" but it's not the concern of this part.
	 *
	 * @param pass Password
	 */
	public void setPassword(String pass) {
		setProperty(PROPERTY_PASSWORD, pass);
	}

	public UUID getDomainId() {
		return getPropertyUUID(PROPERTY_DOMAIN);
	}

	public Domain getDomain() {
		return new Domain(getDomainId());
	}

	public void setDomain(Domain domain) {
		Domain previousDomain = getDomain();
		if (previousDomain != null) {
			previousDomain.removeUser(getId());
		}
		setProperty(PROPERTY_DOMAIN, domain.getId());
		getDomain().addUser(getId());
	}

	@Override
	protected int getObjectVersion() {
		return 1;
	}

	private RegistryNode settings;

	public RegistryNode getSettings() {
		if (settings == null) {
			settings = node.getChild(NODE_SETTINGS);
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

	@Override
	public User check() {
		super.check();
		return this;
	}
}
