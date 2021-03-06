package org.m2mp.db.registry.file;

import java.io.IOException;
import java.io.OutputStream;

/**
 * DBFile's OutputStream
 *
 * @author Florent Clairambault
 */
public class DbFileOutputStream extends OutputStream {

    private final DbFile file;
    private final int blockSize;
    private long size;
    private long offset;
    private final byte[] chunk;
    private int chunkOffset;
    private int chunkNb;

    public DbFileOutputStream(DbFile file) {
        this(file, false);
    }

    public DbFileOutputStream(DbFile file, boolean append) {
        this.file = file;
        this.blockSize = file.getBlockSize();
        this.chunk = new byte[blockSize];

        if (append) {
            size = file.getSize();
            seek(size);
        } else {
            size = 0;
            offset = 0;
        }

        if (!file.exists()) {
            file.create();
        } else {
            file.setOk(false);
        }
    }

    @Override
    public void write(int b) throws IOException {
        offset++;
        this.chunk[chunkOffset++] = (byte) b;

        if (chunkOffset == this.blockSize) {
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
        chunkNb = (int) (offset / blockSize);
        chunkOffset = (int) (offset - (chunkNb * blockSize));

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
        if (offset > size) {
            size = offset;
            file.setSize(size);
        }
    }

    @Override
    public void close() {
        flush();
        file.setOk(true);
    }
}
