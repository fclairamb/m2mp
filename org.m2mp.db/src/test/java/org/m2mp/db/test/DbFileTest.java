package org.m2mp.db.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.common.GeneralSetting;
import org.m2mp.db.registry.RegistryNode;
import org.m2mp.db.registry.file.DbFile;
import org.m2mp.db.registry.file.DbFileInputStream;
import org.m2mp.db.registry.file.DbFileOutputStream;

/**
 *
 * @author Florent Clairambault
 *
 * This test is very agressive for any cassandra cluster since it destroyes and
 * recreates all the tables.
 */
public class DbFileTest {

	public static class Hashing {

		private static String bytes2String(byte[] bytes) {
			StringBuilder string = new StringBuilder(40);
			for (byte b : bytes) {
				String hexString = Integer.toHexString(0x00FF & b);
				string.append(hexString.length() == 1 ? "0" + hexString : hexString);
			}
			return string.toString();
		}

		private static byte[] hash(InputStream is, String hash) throws NoSuchAlgorithmException, IOException {
			MessageDigest md = MessageDigest.getInstance(hash);
			byte[] buffer = new byte[8192];
			int read;
			while ((read = is.read(buffer)) > 0) {
				md.update(buffer, 0, read);
			}
			return md.digest();
		}

		public static String sha1(InputStream is) {
			try {
				return bytes2String(hash(is, "SHA-1"));
			} catch (NoSuchAlgorithmException | IOException ex) {
				Logger.getLogger(Hashing.class.getName()).log(Level.SEVERE, null, ex);
				return null;
			}
		}
	}
//	private static DB db;

	@BeforeClass
	public static void setUpClass() {
		DB.keyspace("ks_test", true);
		try {
			DB.execute("drop table RegistryNode;");
			DB.execute("drop table RegistryNodeChildren;");
			DB.execute("drop table RegistryNodeData;");
		} catch (Exception ex) {
		}
		GeneralSetting.prepareTable();
		DbFile.prepareTable();
	}

	private void writeFile(OutputStream os, int nbBlocks, int blockSize, int mod) throws IOException {
		for (int i = 0; i < nbBlocks; i++) {
			byte[] data = new byte[blockSize];
			for (int j = 0; j < data.length; j++) {
				data[j] = (byte) (j % mod);
			}
			os.write(data);
		}
	}

	@Test
	public void cmpTinySize() throws Exception { // 20 bytes
		realAndDbFilesComparison(1, 20);
	}

	@Test
	public void cmpSmallSize() throws Exception { // 1 MB
		realAndDbFilesComparison(1024, 1024);
	}

	@Test
	public void cmpMediumSize() throws Exception { // 4 MB
		realAndDbFilesComparison(4, 1024 * 1024);
	}

	@Test
	public void cmpBigSize() throws Exception { // 10 MB
		realAndDbFilesComparison(10, 1024 * 1024);
	}

	@Test
	public void skipBytes() throws Exception {
		int nbBlocks = 4, blockSize = 32 * 1024;
		long length = nbBlocks * blockSize;

		File file1;
		DbFile file2;

		{ // File on disk
			file1 = new File("/tmp/test-" + (nbBlocks * blockSize));
			file1.deleteOnExit();
			try (OutputStream os = new FileOutputStream(file1)) {
				writeFile(os, nbBlocks, blockSize, 256);
			}
		}
		{ // File on DB
			file2 = new DbFile(new RegistryNode("/this/is/my/file-" + (nbBlocks * blockSize)).check());
			try (OutputStream os = new DbFileOutputStream(file2)) {
				writeFile(os, nbBlocks, blockSize, 256);
			}
		}

		try (InputStream is1 = new FileInputStream(file1); InputStream is2 = new DbFileInputStream(file2)) {
			is1.skip(length / 2);
			is2.skip(length / 2);
			String hash1 = Hashing.sha1(is1);
			String hash2 = Hashing.sha1(is2);
			Assert.assertEquals(hash1, hash2);
			Assert.assertEquals(hash1, "f04977267a391b2c8f7ad8e070f149bc19b0fc25");
		}

		try (InputStream is1 = new FileInputStream(file1); InputStream is2 = new DbFileInputStream(file2)) {
			is1.skip(2588); // Arbirary number
			is2.skip(2588);
			String hash1 = Hashing.sha1(is1);
			String hash2 = Hashing.sha1(is2);
			Assert.assertEquals(hash1, hash2);
			Assert.assertEquals(hash1, "93f05113e12bd875fe7075fd7e662c8e2f76dbb0");
		}
	}

	@Test
	public void properties() throws Exception {
		{
			DbFile file = new DbFile(new RegistryNode("/this/is/my/file").check());
			file.setName("toto.txt");
			file.setType("my-own/mime-type");
		}
		{
			DbFile file = new DbFile(new RegistryNode("/this/is/my/file").check());
			Assert.assertEquals("toto.txt", file.getName());
			Assert.assertEquals("my-own/mime-type", file.getType());
		}
	}

	private void realAndDbFilesComparison(int nbBlocks, int blockSize) throws Exception {
		File file1;
		DbFile file2;
		{ // File on disk
			file1 = new File("/tmp/test-" + (nbBlocks * blockSize));
			file1.deleteOnExit();
			try (OutputStream os = new FileOutputStream(file1)) {
				writeFile(os, nbBlocks, blockSize, 256);
			}
		}
		{ // File on DB
			file2 = new DbFile(new RegistryNode("/this/is/my/file-" + (nbBlocks * blockSize)).check());
			try (OutputStream os = new DbFileOutputStream(file2)) {
				writeFile(os, nbBlocks, blockSize, 256);
			}
		}
		//This was useful for debugging but it turns out to be quite slow
		if (0 == 1) { // (it's an IDE thing)
			try (InputStream is1 = new FileInputStream(file1); InputStream is2 = new DbFileInputStream(file2)) {
				long offset = 0;
				try {
					while (is1.available() > 0 || is2.available() > 0) {
						Assert.assertEquals(is1.read(), is2.read());
						offset += 1;
					}
				} finally {
					System.out.println("offset = " + offset);
				}
			}
		}
		Assert.assertEquals(file1.length(), file2.getSize());

		{ // Hashing
			String hash1, hash2;
			try (InputStream is = new FileInputStream(file1)) {
				hash1 = Hashing.sha1(is);
			}
			try (InputStream is = new DbFileInputStream(file2)) {
				hash2 = Hashing.sha1(is);
			}
			Assert.assertEquals(hash1, hash2);
		}
	}
}
