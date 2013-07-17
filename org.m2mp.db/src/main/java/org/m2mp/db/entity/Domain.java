package org.m2mp.db.entity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DBAccess;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.registry.RegistryNode;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 */
public class Domain extends Entity {

	private final DBAccess db;
	private UUID domainId;
	private static final String PREFIX = "/domain/";

	public Domain(DBAccess db, UUID id) {
		this.db = db;
		domainId = id;
		node = new RegistryNode(db, PREFIX + id);
	}

	public Domain(DBAccess db, String name) {
		this.db = db;
		domainId = getIdFromName(db, name);
		if (domainId != null) {
			node = new RegistryNode(db, PREFIX + domainId);
		} else {
			throw new IllegalArgumentException("The domain \"" + name + "\" could not be found !");
		}
	}

	public static Domain get(DBAccess db, String name) {
		UUID domainId = getIdFromName(db, name);
		return domainId != null ? new Domain(db, domainId) : null;
	}
	private static final String PROP_NAME = "name";
	private static final String PROP_CREATED_DATE = "created";

	public static Domain create(DBAccess db, String name) {
		UUID domainId = getIdFromName(db, name);
		if (domainId != null) {
			throw new IllegalArgumentException("The domain \"" + name + "\" already exists for domain \"" + domainId + "\"");
		}
		domainId = UUID.randomUUID();
		db.execute(db.prepare("INSERT INTO " + TABLE + " ( name, id ) VALUES ( ?, ? );").bind(name, domainId));

		Domain d = new Domain(db, domainId);
		d.check();
		d.setProperty(PROP_NAME, name);
		d.setProperty(PROP_CREATED_DATE, System.currentTimeMillis());
		return d;
	}

	public String getName() {
		return getProperty(PROP_NAME, null);
	}

	public void setName(String name) {
	}
	public static final String TABLE = "Domain";

	protected static UUID getIdFromName(DBAccess db, String name) {
		ResultSet rs = db.execute(db.prepare("SELECT id FROM " + TABLE + " WHERE name = ?;").bind(name));
		for (Row row : rs) {
			return row.getUUID(0);
		}
		return null;
	}

	public UUID getId() {
		return domainId;
	}

	public static void prepareTable(DBAccess db) {
		RegistryNode.prepareTable(db);
		TableCreation.checkTable(db, new TableIncrementalDefinition() {
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
}
