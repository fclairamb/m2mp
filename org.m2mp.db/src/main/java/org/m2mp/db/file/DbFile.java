package org.m2mp.db.file;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.m2mp.db.Shared;
import org.m2mp.db.common.Entity;
import org.m2mp.db.common.TableCreation;
import org.m2mp.db.common.TableIncrementalDefinition;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author florent
 */
public class DbFile extends Entity {

	private final static String PROPERTY_NAME = "fname";
	private final static String PROPERTY_TYPE = "ftype";
	private final static String PROPERTY_SIZE = "fsize";
	private final static String PROPERTY_CHUNK_SIZE = "chunkSize";
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
	private static final int DEFAULT_CHUNK_SIZE = 256 * 1024;

	public int getChunkSize() {
		int chunckSize = getProperty(PROPERTY_CHUNK_SIZE, -1);
		if (chunckSize == -1) {
			chunckSize = DEFAULT_CHUNK_SIZE;
			setProperty(PROPERTY_CHUNK_SIZE, chunckSize);
		}
		return chunckSize;
	}

	public void setChunkSize(int size) {
		setProperty(PROPERTY_CHUNK_SIZE, size);
	}
	// <editor-fold defaultstate="collapsed" desc="Raw block handling">
	private static PreparedStatement _reqGetBlock;

	private static PreparedStatement reqGetBlock() {
		if (_reqGetBlock == null) {
			_reqGetBlock = Shared.db().prepare("SELECT data FROM " + TABLE_REGISTRYDATA + " WHERE path = ? AND block = ?;");
		}
		return _reqGetBlock;
	}
	private static PreparedStatement _reqSetBlock;

	private static PreparedStatement reqSetBlock() {
		if (_reqSetBlock == null) {
			_reqSetBlock = Shared.db().prepare("INSERT INTO " + TABLE_REGISTRYDATA + " ( path, block, data ) VALUES ( ?, ?, ? );");
		}
		return _reqSetBlock;
	}

	public void setBlock(int blockNb, byte[] data) {
		setBlock(blockNb, ByteBuffer.wrap(data));
	}

	public void setBlock(int blockNb, ByteBuffer data) {
		Shared.db().execute(reqSetBlock().bind(path, blockNb, data));
	}

	public ByteBuffer getBlockBuffer(int blockNb) {
		ResultSet rs = Shared.db().execute(reqGetBlock().bind(path, blockNb));
		for (Row row : rs) {
			return row.getBytes(0);
		}
		return null;
	}

	public byte[] getBlockBytes(int blockNb) {
		ByteBuffer buf = getBlockBuffer(blockNb);
		return buf != null ? buf.array() : null;
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
				list.add(new TableIncrementalDefinition.TableChange(1, "CREATE TABLE " + TABLE_REGISTRYDATA + " ( path text, block int, data blob, PRIMARY KEY( path, block ) ) WITH CLUSTERING ORDER BY ( block ASC );"));
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
}
