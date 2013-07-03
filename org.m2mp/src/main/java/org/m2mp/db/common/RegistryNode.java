/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.common;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author florent
 *
 * The registry node is a kind of "does it all" class. It's very convenient to
 * store pretty much anything.
 */
public class RegistryNode {

	protected String path;

	public RegistryNode(String path) {
		this.path = path;
	}
	private static final String TABLE_REGISTRY = "Registry";
	public static final TableIncrementalDefinition DEFINITION = new TableIncrementalDefinition() {
		public String getTableDefName() {
			return TABLE_REGISTRY;
		}

		public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
			List<TableIncrementalDefinition.TableChange> list = new ArrayList<TableIncrementalDefinition.TableChange>();
			list.add(new TableIncrementalDefinition.TableChange(1, ""
					+ "CREATE TABLE " + TABLE_REGISTRY + " (\n"
					+ "  path text PRIMARY KEY,\n"
					+ "  cache map<text, text>,\n"
					+ "  children set<text>,\n"
					+ "  data map<int, blob>,\n"
					+ "  meta map<text, text>,\n"
					+ "  properties map<text, text>,\n"
					+ "  values map<text, text>\n"
					+ ");"));
			return list;
		}

		public String getTablesDefCql() {
			return ""
					+ "CREATE TABLE " + TABLE_REGISTRY + " (\n"
					+ "  path text PRIMARY KEY,\n"
					+ "  cache map<text, text>,\n"
					+ "  children set<text>,\n"
					+ "  data map<int, blob>,\n"
					+ "  meta map<text, text>,\n"
					+ "  properties map<text, text>,\n"
					+ "  values map<text, text>\n"
					+ ");";
		}

		public int getTableDefVersion() {
			return 1;
		}
	};
}
