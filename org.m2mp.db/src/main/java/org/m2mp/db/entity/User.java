package org.m2mp.db.entity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DB;
import static org.m2mp.db.entity.Domain.getIdFromName;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

/**
 * User data container.
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
	private static final String PROP_DOMAIN = "domain";
	private static final String PROP_CREATED_DATE = "created";
	private static final String PROP_PASSWORD = "password";

	public static User create(String name, Domain domain) {
		UUID userId = getIdFromName(name);
		if (userId != null) {
			throw new IllegalArgumentException("The user \"" + name + "\" already exists with id \"" + userId + "\"");
		}
		userId = UUID.randomUUID();
		DB.execute(DB.prepare("INSERT INTO " + TABLE + " ( name, id, domain ) VALUES ( ?, ?, ? );").bind(name, userId, domain.getId()));
		User u = new User(userId);
		u.check();
		u.setProperty(PROP_NAME, name);
		u.setProperty(PROP_CREATED_DATE, System.currentTimeMillis());
		u.setProperty(PROP_DOMAIN, domain.getId().toString());
		return u;
	}

//	@Override
//	public User check() {
//		super.check();
//		return this;
//	}
	public String getPassword() {
		return getProperty(PROP_PASSWORD, null);
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
}
