package org.m2mp.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Generic streams management helper.
 */
public class Streams {

	private static final int IO_BUFFER_SIZE = 8 * 1024;

	public static void copy(InputStream in, OutputStream out) throws IOException {
		byte[] b = new byte[IO_BUFFER_SIZE];
		int read;
		while ((read = in.read(b)) != -1) {
			out.write(b, 0, read);
		}
	}

	/**
	 * Copy a stream to an other stream.
	 * @param in Input stream
	 * @param out Output stream
	 * @param length Bytes to copy
	 * @return Remaining bytes
	 * @throws IOException 
	 */
	public static long copy(InputStream in, OutputStream out, long length) throws IOException {
		byte[] b = new byte[IO_BUFFER_SIZE];
		int read;
		while ((read = in.read(b)) != -1 && length > 0) {
			if (read > length) {
				read = (int) length;
			}
			length -= read;
			out.write(b, 0, read);
		}
		return length;
	}
}
