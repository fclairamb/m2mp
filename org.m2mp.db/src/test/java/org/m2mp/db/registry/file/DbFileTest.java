package org.m2mp.db.registry.file;

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
	public void TinySize() throws Exception { // 20 bytes
		realAndDbFilesComparison(1, 20, 4096);
	}

	@Test
	public void SmallSize() throws Exception { // 1 MB
		realAndDbFilesComparison(1, 20, 1024 * 512);
	}

	@Test
	public void MediumSize() throws Exception { // 4 MB
		realAndDbFilesComparison(1, 20, 1024 * 512);
	}

	@Test
	public void BigSize() throws Exception { // 20 MB
		realAndDbFilesComparison(20, 1024 * 1024, 1024 * 512);
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

	private void realAndDbFilesComparison(int nbBlocks, int blockSize, int chunkSize) throws Exception {
		File file1;
		DbFile file2;
		{ // File on disk
			file1 = new File("/tmp/test-" + (nbBlocks * blockSize) + "-" + chunkSize);
			file1.deleteOnExit();
			try (OutputStream os = new FileOutputStream(file1)) {
				writeFile(os, nbBlocks, blockSize, 256);
			}
		}
		{ // File on DB
			file2 = new DbFile(new RegistryNode("/this/is/my/file-" + (nbBlocks * blockSize) + "-" + chunkSize).check());
			file2.setBlockSize(chunkSize);
			try (OutputStream os = new DbFileOutputStream(file2)) {
				writeFile(os, nbBlocks, blockSize, 256);
			}
		}
		//This was useful for debugging but it turns out to be quite slow
		if (0 == 1) { // (it's an IDE thing)
			try (InputStream is1 = new FileInputStream(file1)) {
				try (InputStream is2 = new DbFileInputStream(file2)) {
					long offset = 0;
					try {
						while (is1.available() > 0 && is2.available() > 0) {
							Assert.assertEquals(is1.read(), is2.read());
							offset += 1;
						}
					} finally {
						System.out.println("offset = " + offset);
					}
					Assert.assertEquals(0, is1.available());
					Assert.assertEquals(0, is2.available());
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
