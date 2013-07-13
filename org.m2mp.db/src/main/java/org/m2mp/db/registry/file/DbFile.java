package org.m2mp.db.registry.file;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.DBAccess;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

/**
 * File stored in database.
 *
 * @author Florent Clairambault
 *
 * <strong>Notes:</strong><br />
 * <ul>
 * <li>Requesting </li>
 * </ul>
 */
public class DbFile extends Entity {

	private final static String PROPERTY_NAME = "fname";
	private final static String PROPERTY_TYPE = "ftype";
	private final static String PROPERTY_SIZE = "fsize";
	private final static String PROPERTY_BLOCK_SIZE = "blsize";
	final String path;

	public DbFile(RegistryNode node) {
		this.node = node;
		this.path = node.getPath();
	}

	public void setType(String type) {
		setProperty(PROPERTY_TYPE, type);
	}

	public String getType() {
		return getProperty(PROPERTY_TYPE, null);
	}

	public void setName(String filename) {
		setProperty(PROPERTY_NAME, filename);
	}

	public String getName() {
		return getProperty(PROPERTY_NAME, null);
	}

	public void setSize(long size) {
		setProperty(PROPERTY_SIZE, size);
	}

	public long getSize() {
		return getProperty(PROPERTY_SIZE, (long) 0);
	}
	private static final int DEFAULT_CHUNK_SIZE = 512 * 1024;

	public int getBlockSize() {
		int chunckSize = getProperty(PROPERTY_BLOCK_SIZE, -1);
		if (chunckSize == -1) {
			chunckSize = DEFAULT_CHUNK_SIZE;
			setProperty(PROPERTY_BLOCK_SIZE, chunckSize);
		}
		return chunckSize;
	}

	public void setBlockSize(int size) {
		setProperty(PROPERTY_BLOCK_SIZE, size);
	}
	// <editor-fold defaultstate="collapsed" desc="Raw block handling">

	public void delBlock(int blockNb) {
		node.getDb().execute(reqDelBlock().bind(path, blockNb));
	}

	public void setBlock(int blockNb, byte[] data) {
		setBlock(blockNb, ByteBuffer.wrap(data));
	}

	public void setBlock(int blockNb, ByteBuffer data) {
		//System.out.println("Writing block " + path + ":" + blockNb);
		node.getDb().execute(reqSetBlock().bind(path, blockNb, data));
	}

	public ByteBuffer getBlockBuffer(int blockNb) {
		ResultSet rs = node.getDb().execute(reqGetBlock().bind(path, blockNb));
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

	public static void prepareTable(DBAccess db) {
		RegistryNode.prepareTable(db);
		TableCreation.checkTable(db, new TableIncrementalDefinition() {
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
			reqGetBlock = node.getDb().prepare("SELECT data FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
		}
		return reqGetBlock;
	}
	private PreparedStatement reqGetBlock;

	public PreparedStatement reqSetBlock() {
		if (reqSetBlock == null) {
			reqSetBlock = node.getDb().prepare("INSERT INTO " + TABLE_REGISTRYDATA + " ( path, block, data ) VALUES ( ?, ?, ? );");
		}
		return reqSetBlock;
	}
	private PreparedStatement reqSetBlock;

	public PreparedStatement reqDelBlock() {
		if (reqDelBlock == null) {
			reqDelBlock = node.getDb().prepare("DELETE FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
		}
		return reqDelBlock;
	}
	private PreparedStatement reqDelBlock;
}
