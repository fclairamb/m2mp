/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.file;

import java.io.IOException;
import java.io.OutputStream;
import org.m2mp.db.registry.RegistryNode;

/**
 *
 * @author Florent Clairambault
 */
public class DbFileOutputStream extends OutputStream {

	private final DbFile file;
	private final int chunkSize;
	private long size;
	private long offset;
	private final byte[] chunk;
	private int chunkOffset;
	private int chunkNb;
	private final static String PROPERTY_SIZE = "size";

	public DbFileOutputStream(DbFile file) {
		this.file = file;
		this.chunkSize = file.getBlockSize();
		this.size = file.getSize();
		this.chunk = new byte[chunkSize];

		this.offset = 0;
	}

	@Override
	public void write(int b) throws IOException {
		offset++;
		this.chunk[chunkOffset++] = (byte) b;

		if (chunkOffset == this.chunkSize) {
			write();
		}
	}

	public void seek(long offset) {
		// If we already defined something, we write what we should have written
		if (this.offset != 0) {
			write();
		}

		this.offset = offset;

		// We define our new chunk
		chunkNb = (int) (offset / chunkSize);
		chunkOffset = (int) (offset - (chunkNb * chunkSize));

		// But we also need to load some possibly existing data
		byte[] previousChunk = file.getBlockBytes(chunkNb);
		if (previousChunk != null) {
			System.arraycopy(previousChunk, 0, chunk, 0, previousChunk.length);
		}
	}

	@Override
	public void flush() {
		write();
	}

	private void write() {
		if (chunkOffset != chunk.length) {
			byte[] smallerChunk = new byte[chunkOffset];
			System.arraycopy(chunk, 0, smallerChunk, 0, chunkOffset);
			file.setBlock(chunkNb, smallerChunk);
		} else {
			file.setBlock(chunkNb, chunk);
			chunkNb++;
			chunkOffset = 0;
		}
//		db.setFileProperty( path, "size", ""+size);
		if (offset > size) {
			size = offset;
			file.setSize(size);
		}

	}

	@Override
	public void close() {
		flush();
	}
}
