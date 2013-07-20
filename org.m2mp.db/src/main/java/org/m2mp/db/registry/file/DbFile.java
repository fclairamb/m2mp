package org.m2mp.db.registry.file;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.DB;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

/**
 * File stored in database.
 *
 * The current implementation is minimalistic. It doesn't take advantage of
 * async mechanisms or the NIO buffers the new cassandra driver offers.
 *
 * @author Florent Clairambault
 */
public class DbFile extends Entity {

	private final static String PROPERTY_NAME = "fname";
	private final static String PROPERTY_TYPE = "ftype";
	private final static String PROPERTY_SIZE = "fsize";
	private final static String PROPERTY_BLOCK_SIZE = "blsize";
	final String path;

	/**
	 * Constructor.
	 *
	 * @param node Node to wrap the DbFile around
	 */
	public DbFile(RegistryNode node) {
		this.node = node;
		this.path = node.getPath();
	}

	/**
	 * Set the type of file (mime-type)
	 *
	 * @param type Type
	 */
	public void setType(String type) {
		setProperty(PROPERTY_TYPE, type);
	}

	/**
	 * Get the type of file (mime-type)
	 *
	 * @return Type
	 */
	public String getType() {
		return getProperty(PROPERTY_TYPE, null);
	}

	/**
	 * Set the name of the file (as it should be returned)
	 *
	 * @param filename Name of the file
	 */
	public void setName(String filename) {
		setProperty(PROPERTY_NAME, filename);
	}

	/**
	 * Get the name of file
	 *
	 * @return Name of the file
	 */
	public String getName() {
		return getProperty(PROPERTY_NAME, null);
	}

	/**
	 * Set the size of the file
	 *
	 * @param size Size of the file
	 */
	void setSize(long size) {
		setProperty(PROPERTY_SIZE, size);
	}

	/**
	 * Get the size of the file.
	 *
	 * @return Size of the file
	 */
	public long getSize() {
		return getProperty(PROPERTY_SIZE, (long) 0);
	}
	private static final int DEFAULT_BLOCK_SIZE = 512 * 1024;

	/**
	 * Get the block size. Each file might have a different block size.
	 *
	 * @return
	 */
	int getBlockSize() {
		int blockSize = getProperty(PROPERTY_BLOCK_SIZE, -1);
		if (blockSize == -1) {
			blockSize = DEFAULT_BLOCK_SIZE;
			setProperty(PROPERTY_BLOCK_SIZE, blockSize);
		}
		return blockSize;
	}

	/**
	 * Set the block size.
	 *
	 * Each block is a new column in the actual
	 *
	 * @return
	 */
	void setBlockSize(int size) {
		setProperty(PROPERTY_BLOCK_SIZE, size);
	}
	// <editor-fold defaultstate="collapsed" desc="Raw block handling">

	public void delBlock(int blockNb) {
		DB.execute(reqDelBlock().bind(path, blockNb));
	}

	public void setBlock(int blockNb, byte[] data) {
		setBlock(blockNb, ByteBuffer.wrap(data));
	}

	public void setBlock(int blockNb, ByteBuffer data) {
		//System.out.println("Writing block " + path + ":" + blockNb);
		DB.execute(reqSetBlock().bind(path, blockNb, data));
	}

	public ByteBuffer getBlockBuffer(int blockNb) {
		ResultSet rs = DB.execute(reqGetBlock().bind(path, blockNb));
		for (Row row : rs) {
			return row.getBytes(0);
		}
		return null;
	}

	public byte[] getBlockBytes(int blockNb) {
		//System.out.println("Reading block " + path + ":" + blockNb);
		ByteBuffer buf = getBlockBuffer(blockNb);
		if (buf == null) {
			return null;
		}
		// This is crappy. It breaks the whole ByteBuffer idea
		byte[] array = new byte[(buf.array().length - buf.position())];
		System.arraycopy(buf.array(), buf.position(), array, 0, array.length);
		return array;
	}
	// </editor-fold>
	// <editor-fold defaultstate="collapsed" desc="Column family preparation">
	private static final String TABLE_REGISTRYDATA = RegistryNode.TABLE_REGISTRY + "Data";

	public static void prepareTable() {
		RegistryNode.prepareTable();
		TableCreation.checkTable(new TableIncrementalDefinition() {
			@Override
			public String getTableDefName() {
				return TABLE_REGISTRYDATA;
			}

			@Override
			public List<TableIncrementalDefinition.TableChange> getTableDefChanges() {
				List<TableIncrementalDefinition.TableChange> list = new ArrayList<>();
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_REGISTRYDATA + " ( path text, block int, data blob, PRIMARY KEY( path, block ) ) WITH CLUSTERING ORDER BY ( block ASC ) AND compression={'sstable_compression':''};"));
				return list;
			}

			@Override
			public int getTableDefVersion() {
				return 1;
			}
		});
	}
	// </editor-fold>

	public InputStream openInputStream() {
		return new DbFileInputStream(this);
	}

	public OutputStream openOutputStream() {
		return new DbFileOutputStream(this);
	}

	public void delete() {
		node.delete();
	}

	public PreparedStatement reqGetBlock() {
		if (reqGetBlock == null) {
			reqGetBlock = DB.prepare("SELECT data FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
		}
		return reqGetBlock;
	}
	private PreparedStatement reqGetBlock;

	public PreparedStatement reqSetBlock() {
		if (reqSetBlock == null) {
			reqSetBlock = DB.prepare("INSERT INTO " + TABLE_REGISTRYDATA + " ( path, block, data ) VALUES ( ?, ?, ? );");
		}
		return reqSetBlock;
	}
	private PreparedStatement reqSetBlock;

	public PreparedStatement reqDelBlock() {
		if (reqDelBlock == null) {
			reqDelBlock = DB.prepare("DELETE FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
		}
		return reqDelBlock;
	}
	private PreparedStatement reqDelBlock;
}
