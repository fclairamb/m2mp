/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author florent
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

	public static void copy(InputStream in, OutputStream out, long length) throws IOException {
		byte[] b = new byte[IO_BUFFER_SIZE];
		int read;
		while ((read = in.read(b)) != -1 && length > 0) {
			if (read > length) {
				read = (int) length;
			}
			length -= read;
			out.write(b, 0, read);
		}
	}
}