package org.m2mp.db.entity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DBAccess;
import static org.m2mp.db.entity.Domain.getIdFromName;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author Florent Clairambault
 */
public class User extends Entity {

	private DBAccess db;
	private UUID userId;
	private static final String PREFIX = "/user/";

	public User(DBAccess db, UUID userId) {
		this.db = db;
		this.userId = userId;
		node = new RegistryNode(db, PREFIX + userId);
	}

	protected static UUID getIdFromName(DBAccess db, String name) {
		ResultSet rs = db.execute(db.prepare("SELECT id FROM " + TABLE + " WHERE name = ?;").bind(name));
		for (Row row : rs) {
			return row.getUUID(0);
		}
		return null;
	}

	public UUID getId() {
		return userId;
	}

	public static User get(DBAccess db, String name) {
		UUID userId = getIdFromName(db, name);
		return userId != null ? new User(db, userId) : null;
	}
	private static final String PROP_NAME = "name";
	private static final String PROP_DOMAIN = "domain";
	private static final String PROP_CREATED_DATE = "created";
	private static final String PROP_PASSWORD = "password";

	public static User create(DBAccess db, String name, Domain domain) {
		UUID userId = getIdFromName(db, name);
		if (userId != null) {
			throw new IllegalArgumentException("The user \"" + name + "\" already exists with id \"" + userId + "\"");
		}
		userId = UUID.randomUUID();
		db.execute(db.prepare("INSERT INTO " + TABLE + " ( name, id, domain ) VALUES ( ?, ?, ? );").bind(name, userId, domain.getId()));
		User u = new User(db, userId);
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

	public static void prepareTable(DBAccess db) {
		Domain.prepareTable(db);
		TableCreation.checkTable(db, new TableIncrementalDefinition() {
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
