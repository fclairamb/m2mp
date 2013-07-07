/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import java.io.OutputStream;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.Shared;
import org.m2mp.db.file.DbFile;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author florent
 */
public class DbFiles {

	@BeforeClass
	public static void setUpClass() {
		BaseTest.setUpClass();
		try {
			Shared.db().execute("drop table RegistryNode;");
			Shared.db().execute("drop table RegistryNodeChildren;");
			Shared.db().execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		DbFile.prepareTable();
	}

	@Test
	public void copy() throws Exception {
		DbFile file = new DbFile(new RegistryNode("/this/is/my/file"));
		file.check();
		file.setChunkSize(512);
		try (OutputStream os = file.openOutputStream()) {
			for (int i = 0; i < 512; i++) {
				byte[] data = new byte[1024];

				for (int j = 0; j < data.length; j++) {
					data[j] = (byte) (j % 100);
				}
				os.write(data);
			}
		}
	}
}
