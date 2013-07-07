package org.m2mp.db.file;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Florent Clairambault
 */
public class DbFileInputStream extends InputStream {

	private final DbFile file;
	private final int chunkSize;
	private final long size;
	private byte[] chunk;
	private long readOffset;
	private int chunkOffset;
	private int chunkNb;

	public DbFileInputStream(DbFile file) {
		this.file = file;
		this.chunkSize = file.getBlockSize();
		this.size = file.getSize();
	}

	@Override
	public int read() throws IOException {
		// We must never go over the current offset
		if (readOffset++ == size) {
			return -1;
		}

		if (chunkOffset == 0 || chunkOffset == chunk.length) {
			getNextChunk();
		}


		if (chunk != null && chunkOffset < chunk.length) {
			return (int) chunk[chunkOffset++] & 0xFF;
		} else {
			return -1;
		}
	}

	@Override
	public int available() {
		return (int) (size - readOffset);
	}

	private void getChunk(int nb) {
		this.chunk = file.getBlockBytes(nb);
	}

	private void getNextChunk() {
		getChunk(chunkNb++);
		chunkOffset = 0;
	}

	@Override
	public synchronized void reset() throws IOException {
		readOffset = 0;
		chunkNb = 0;
		chunkOffset = 0;
	}

	@Override
	public long skip(long n) throws IOException {
		if (readOffset + n > size) {
			n = size - readOffset;
		}

		setReadOffset(readOffset + n);

		return n;
	}

	private void setReadOffset(long offset) {
		readOffset = offset;
		int newChunk = (int) (readOffset / chunkOffset);
		if (newChunk != chunkNb) {
			chunkNb = newChunk;
			getChunk(chunkNb);
		}
		chunkOffset = (int) (readOffset - (chunkSize * chunkNb));
	}
}
