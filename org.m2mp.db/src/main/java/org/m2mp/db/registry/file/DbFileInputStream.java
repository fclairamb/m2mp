package org.m2mp.db.registry.file;

import java.io.IOException;
import java.io.InputStream;

/**
 * DBFile's inputstream.
 *
 * @author Florent Clairambault
 * 
 * (We don't declare AutoCloseable as closing makes no sense in the DbFileInputStream)
 */
public class DbFileInputStream extends InputStream {

	private final DbFile file;
	private final int blockSize;
	private final long size;
	private byte[] block;
	private long readOffset;
	private int blockOffset;
	private int blockNb = -1;

	/**
	 * DBFile's InputStream constructor.
	 *
	 * @param file DbFile
	 */
	public DbFileInputStream(DbFile file) {
		this.file = file;
		this.blockSize = file.getBlockSize();
		this.size = file.getSize();
	}

	@Override
	public int read() throws IOException {
		// If we reached the end, it's over
		if (readOffset++ >= size) {
			return -1;
		}

		// If we're on the beginning or the end of the block, we must get the next block
		if (blockOffset == 0 || blockOffset == block.length) {
			getNextChunk();
		}


		if (block != null && blockOffset < block.length) {
			return (int) block[blockOffset++] & 0xFF;
		} else {
			return -1;
		}
	}
	
	@Override
	public int available() {
		return (int) (size - readOffset);
	}

	private void getChunk(int nb) {
		this.block = file.getBlockBytes(nb);
	}

	private void getNextChunk() {
		getChunk(blockNb++);
		blockOffset = 0;
	}

	@Override
	public synchronized void reset() throws IOException {
		readOffset = 0;
		blockNb = 0;
		blockOffset = 0;
	}

	@Override
	public long skip(long n) throws IOException {
		if (readOffset + n > size) {
			n = size - readOffset;
		}

		setReadOffset(readOffset + n);

		return n;
	}

	/**
	 * Defines the read offset. Internal method used to retrieve a block and its
	 * offset for a defined offset.
	 *
	 * @param offset Offset in the file
	 */
	private void setReadOffset(long offset) {
		readOffset = offset;
		int newChunk = (int) (readOffset / blockSize);
		if (newChunk != blockNb) {
			blockNb = newChunk;
			getChunk(blockNb);
		}
		blockOffset = (int) (readOffset - (blockSize * blockNb));
	}
}
