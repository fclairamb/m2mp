package org.m2mp.db;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author Florent Clairambault
 */
public class User extends Entity {

	private UUID userId;

	public User(UUID userId) {
		this.userId = userId;
		node = new RegistryNode("/u/" + userId);
	}
	private static final String TABLE_USER = "User";
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		@Override
		public String getTableDefName() {
			return TABLE_USER;
		}

		@Override
		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
			list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_USER + " name text PRIMARY KEY, id uuid, domain uuid );"));
			return list;
		}

		public String getTablesDefCql() {
			return "CREATE TABLE " + TABLE_USER + " name text PRIMARY KEY, id uuid, domain uuid );";
		}

		@Override
		public int getTableDefVersion() {
			return 1;
		}
	};
}
