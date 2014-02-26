package org.m2mp.db.entity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.m2mp.db.DB;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * User data container.
 *
 * This is mostly a code sample but it can be applied to pretty much all
 * projects.
 *
 * @author Florent Clairambault
 */
public class User extends Entity {

	private UUID userId;
	private static final String PREFIX = "/user/";

	public User(UUID userId) {
		this.userId = userId;
		node = new RegistryNode(PREFIX + userId);
	}

	public User(RegistryNode node) {
		this.node = node;
		this.userId = UUID.fromString(node.getName());
	}

	protected static UUID getIdFromName(String name) {
		ResultSet rs = DB.execute(DB.prepare("SELECT id FROM " + TABLE + " WHERE name = ?;").bind(name));
		for (Row row : rs) {
			return row.getUUID(0);
		}
		return null;
	}

	public UUID getId() {
		return userId;
	}

	public static User get(String name) {
		UUID userId = getIdFromName(name);
		return userId != null ? new User(userId) : null;
	}
	private static final String PROP_NAME = "name";
	private static final String PROP_DISPLAYNAME = "displayName";
	private static final String PROP_DOMAIN = "domain";
	private static final String PROP_CREATED_DATE = "created";
	private static final String PROP_PASSWORD = "password";

	public static User create(String name, Domain domain) {
		UUID userId = getIdFromName(name);
		if (userId != null) {
			throw new IllegalArgumentException("The user \"" + name + "\" already exists with id \"" + userId + "\"");
		}
		userId = UUID.randomUUID();
		User u = new User(userId);
		u.check();
		u.setProperty(PROP_CREATED_DATE, System.currentTimeMillis());
		u.setUsername(name);
		u.setDomain(domain);
		return u;
	}

	/**
	 * Set the username
	 *
	 * @param name Username
	 *
	 * @deprecated We're keeping this a little bit longer
	 */
	public void setUsername(String name) {
		setProperty(PROP_NAME, name);
		DB.execute(DB.prepare("INSERT INTO " + TABLE + " ( name, id ) VALUES ( ?, ? );").bind(name, userId));
	}

	public String getUsername() {
		return getProperty(PROP_NAME, null);
	}

	public String getDisplayName() {
		String name = getProperty(PROP_DISPLAYNAME, null);
		return name != null ? name : getUsername();
	}

	public String getPassword() {
		return getProperty(PROP_PASSWORD, null);
	}

	public static User authenticate(String username, String password) {
		User user = User.get(username);
		return (user != null && password != null && password.equals(user.getPassword())) ? user : null;
	}

	public void setPassword(String pass) {
		setProperty(PROP_PASSWORD, pass);
	}
	public static final String TABLE = "User";

	public static void prepareTable() {
		Domain.prepareTable();
		TableCreation.checkTable(new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE + " ( name text PRIMARY KEY, id uuid, domain uuid );"));
				list.add(new TableIncrementalDefinition.TableChange(2, "CREATE INDEX ON " + TABLE + " ( domain );"));
				return list;
			}

			@Override
			public int getTableDefVersion() {
				return 1;
			}
		});
	}

	public UUID getDomainId() {
		return getPropertyUUID(PROP_DOMAIN);
	}

	public Domain getDomain() {
		return new Domain(getDomainId());
	}

	public void setDomain(Domain domain) {
		setProperty(PROP_DOMAIN, domain.getId());
		DB.execute(DB.prepare("UPDATE " + TABLE + " SET domain = ? WHERE name = ?;").bind(domain.getId(), getUsername()));
	}

	@Override
	protected int getObjectVersion() {
		return 1;
	}

	public RegistryNode settings() {
		return node.getChild("settings").check();
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
		return userId.hashCode();
	}
}
