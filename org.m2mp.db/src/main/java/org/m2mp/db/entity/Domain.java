package org.m2mp.db.entity;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.DB;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.registry.RegistryNode;
import org.m2mp.db.common.TableIncrementalDefinition;

/**
 *
 * @author Florent Clairambault
 */
public class Domain extends Entity {

	private UUID domainId;

	public Domain(UUID id) {
		domainId = id;
		node = new RegistryNode("/d/" + id);
	}

	public Domain(String name) {
		domainId = getIdFromName(name);
		if (domainId != null) {
			node = new RegistryNode("/d/" + domainId);
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
		DB.session().execute(reqInsertDomain().bind(name, domainId));

		Domain d = new Domain(domainId);
		d.check();
		d.setProperty(PROP_NAME, name);
		d.setProperty(PROP_CREATED_DATE, System.currentTimeMillis());
		return d;
	}

	public String getName() {
		return getProperty(PROP_NAME, null);
	}
	private static final String TABLE_DOMAIN = "Domain";

	public static void prepareTable() {
		RegistryNode.prepareTable();
		TableCreation.checkTable(new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE_DOMAIN;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, ""
						+ "CREATE TABLE " + TABLE_DOMAIN + " (\n"
						+ "  name text PRIMARY KEY,"
						+ "  id uuid\n"
						+ ");"));
				return list;
			}

			public String getTablesDefCql() {
				return ""
						+ "CREATE TABLE " + TABLE_DOMAIN + " (\n"
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

	private static PreparedStatement reqGetIdFromName() {
		if (reqGetIdFromName == null) {
			reqGetIdFromName = DB.session().prepare("SELECT id FROM " + TABLE_DOMAIN + " WHERE name = ?;");
		}
		return reqGetIdFromName;
	}
	private static PreparedStatement reqGetIdFromName;

	protected static UUID getIdFromName(String name) {
		ResultSet rs = DB.session().execute(reqGetIdFromName().bind(name));
		for (Row row : rs) {
			return row.getUUID(0);
		}
		return null;
	}

	private static PreparedStatement reqInsertDomain() {
		if (reqInsertDomain == null) {
			reqInsertDomain = DB.session().prepare("INSERT INTO " + TABLE_DOMAIN + " ( name, id ) VALUES ( ?, ? );");
		}
		return reqInsertDomain;
	}
	private static PreparedStatement reqInsertDomain;

	public UUID getId() {
		return domainId;
	}
}
