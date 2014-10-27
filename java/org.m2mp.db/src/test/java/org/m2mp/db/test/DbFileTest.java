package org.m2mp.db.test;

import com.datastax.driver.core.ResultSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.m2mp.db.DB;
import org.m2mp.db.registry.RegistryNode;
import org.m2mp.db.registry.file.DbFile;
import org.m2mp.db.registry.file.DbFileInputStream;
import org.m2mp.db.registry.file.DbFileOutputStream;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Florent Clairambault
 *         <p/>
 *         This test is very agressive for any cassandra cluster since it destroyes and
 *         recreates all the tables.
 */
public class DbFileTest {

    @BeforeClass
    public static void setUpClass() {
        General.setUpClass();
        RegistryNode.dropTable();
        RegistryNode.prepareTable();
    }

    private void writeFile(OutputStream os, int nbBlocks, int blockSize, int mod) throws IOException {
        for (int i = 0; i < nbBlocks; i++) {
            byte[] data = new byte[blockSize];
            for (int j = 0; j < data.length; j++) {
                data[j] = (byte) ((i + j) % mod);
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
            Assert.assertEquals(hash1, "db8caa7f1d67d4c96a7f1f3131976ed31c6200b8");
        }

        try (InputStream is1 = new FileInputStream(file1); InputStream is2 = new DbFileInputStream(file2)) {
            is1.skip(2588); // Arbirary number
            is2.skip(2588);
            String hash1 = Hashing.sha1(is1);
            String hash2 = Hashing.sha1(is2);
            Assert.assertEquals(hash1, hash2);
            Assert.assertEquals(hash1, "8825548a545d511e5bea41cb9a723f9ce4ca449a");
        }
    }

    @Test
    public void skipBytes2() throws Exception {
        int nbBlocks = 2, blockSize = 512 * 1024;

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
            is1.skip(blockSize - 2);
            is2.skip(blockSize - 2);
            String hash1 = Hashing.sha1(is1);
            String hash2 = Hashing.sha1(is2);
            Assert.assertEquals(hash1, hash2);
        }
    }

    @Test
    public void skipZero() throws Exception {
        int nbBlocks = 2, blockSize = 16 * 1024;

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
            is1.skip(0);
            is2.skip(0);
            String hash1 = Hashing.sha1(is1);
            String hash2 = Hashing.sha1(is2);
            Assert.assertEquals(hash1, hash2);
        }
    }

    @Test
    public void readReset() throws Exception {
        int nbBlocks = 2, blockSize = 16 * 1024;

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

        try (InputStream is2 = new DbFileInputStream(file2)) {
            for (int i = 0; i < 2; i++) {
                is2.reset();
                try (InputStream is1 = new FileInputStream(file1)) {
                    String hash1 = Hashing.sha1(is1);
                    String hash2 = Hashing.sha1(is2);
                    Assert.assertEquals(hash1, hash2);
                }
            }
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

    @Test
    public void rewriteToSmaller() throws Exception {
        String name = "/this/is/my/file2";
        { // Some file
            DbFile file = new DbFile(new RegistryNode(name).check());
            try (OutputStream os = file.openOutputStream()) {
                os.write(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06});
            }
            Assert.assertEquals(6, file.getSize());
        }
        { // Some smaller file
            DbFile file = new DbFile(new RegistryNode(name).check());
            try (OutputStream os = file.openOutputStream()) {
                os.write(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05});
            }
            Assert.assertEquals(5, file.getSize());
        }
    }

    @Test
    public void okStatus() throws Exception {
        String name = "/this/is/my/file3";
        { // Not closing it
            DbFile file = new DbFile(new RegistryNode(name).check());
            OutputStream os = file.openOutputStream();
            os.write(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06});
            Assert.assertEquals(0, file.getSize());
            Assert.assertEquals(false, file.getOk());
        }

        { // Closing it
            DbFile file = new DbFile(new RegistryNode(name).check());
            try (OutputStream os = file.openOutputStream()) {
                os.write(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07});
            }
            Assert.assertEquals(7, file.getSize());
            Assert.assertEquals(true, file.getOk());
        }

        { // Not closing it again
            DbFile file = new DbFile(new RegistryNode(name).check());
            OutputStream os = file.openOutputStream();
            os.write(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06});
            Assert.assertEquals(7, file.getSize());
            Assert.assertEquals(false, file.getOk());
        }
    }

    @Test
    public void appendingData() throws Exception {
        String name = "/this/is/my/file4";
        { // Writing 3 bytes
            DbFile file = new DbFile(new RegistryNode(name).check());
            try (OutputStream os = file.openOutputStream()) {
                os.write(new byte[]{0x01, 0x02, 0x03});
            }
            Assert.assertEquals(3, file.getSize());
        }
        { // Writing 2 bytes
            DbFile file = new DbFile(new RegistryNode(name).check());
            try (OutputStream os = new DbFileOutputStream(file, true)) {
                os.write(new byte[]{0x04, 0x05});
            }
            Assert.assertEquals(5, file.getSize());
        }
        { // We must have 5 bytes
            DbFile file = new DbFile(new RegistryNode(name).check());
            try (InputStream is = new DbFileInputStream(file)) {
                byte[] data = new byte[5];
                is.read(data);
                Assert.assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05}, data);
            }
        }
    }

    @Test
    public void deletingEverything() throws Exception {
        cmpSmallSize();

        { // We must have some data
            ResultSet result = DB.execute("SELECT * FROM " + DbFile.TABLE_REGISTRYDATA + ";");
            Assert.assertTrue(result.all().size() > 0);
        }

        new RegistryNode("/").delete(true);

        { // We must NOT have some data
            ResultSet result = DB.execute("SELECT * FROM " + DbFile.TABLE_REGISTRYDATA + ";");
            Assert.assertEquals(0, result.all().size());
        }
        { // We must NOT have some data
            ResultSet result = DB.execute("SELECT * FROM " + RegistryNode.TABLE_REGISTRY + ";");
            Assert.assertEquals(0, result.all().size());
        }
    }

    @Test
    public void deletingEverythingCleanup() throws Exception {
        cmpSmallSize();

        { // We must have some data
            ResultSet result = DB.execute("SELECT * FROM " + DbFile.TABLE_REGISTRYDATA + ";");
            Assert.assertTrue(result.all().size() > 0);
        }

        new RegistryNode("/").delete();

        { // We must have some data
            ResultSet result = DB.execute("SELECT * FROM " + DbFile.TABLE_REGISTRYDATA + ";");
            Assert.assertTrue(result.all().size() > 0);
        }

        RegistryNode.cleanup();

        { // We must NOT have some data
            ResultSet result = DB.execute("SELECT * FROM " + DbFile.TABLE_REGISTRYDATA + ";");
            Assert.assertEquals(0, result.all().size());
        }
        { // We must NOT have some data
            ResultSet result = DB.execute("SELECT * FROM " + RegistryNode.TABLE_REGISTRY + ";");
            Assert.assertEquals(0, result.all().size());
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
}
