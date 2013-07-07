package org.m2mp.db.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Florent Clairambault
 */
public class Hashing {

	private static String bytes2String(byte[] bytes) {
		StringBuilder string = new StringBuilder(40);
		for (byte b : bytes) {
			String hexString = Integer.toHexString(0x00FF & b);
			string.append(hexString.length() == 1 ? "0" + hexString : hexString);
		}
		return string.toString();
	}

	public static byte[] hash(InputStream is, String hash) throws NoSuchAlgorithmException, IOException {
		MessageDigest md = MessageDigest.getInstance(hash);
		byte[] buffer = new byte[8192];
		int read;
		while ((read = is.read(buffer)) > 0) {
			md.update(buffer, 0, read);
		}
		return md.digest();
	}

	private static byte[] hash(byte[] input, String hash) throws NoSuchAlgorithmException {
		return MessageDigest.getInstance(hash).digest(input);
	}

	private static byte[] hash(String input, String hash) throws NoSuchAlgorithmException {
		try {
			return hash(input.getBytes("UTF-8"), hash);
		} catch (UnsupportedEncodingException ex) {
			Logger.getLogger(Hashing.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	public static String sha1(String text) {
		try {
			return bytes2String(hash(text.getBytes("UTF-8"), "SHA-1"));
		} catch (UnsupportedEncodingException | NoSuchAlgorithmException ex) {
			Logger.getLogger(Hashing.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	public static String sha1(InputStream is) {
		try {
			return bytes2String(hash(is, "SHA-1"));
		} catch (NoSuchAlgorithmException | IOException ex) {
			Logger.getLogger(Hashing.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

//  Nobody wants to hear about MD5 anymore
//	public static String md5(InputStream is) {
//		try {
//			return bytes2String(hash(is, "MD5"));
//		} catch (NoSuchAlgorithmException | IOException ex) {
//			Logger.getLogger(Hashing.class.getName()).log(Level.SEVERE, null, ex);
//			return null;
//		}
//	}
	public static UUID sha1ToUUID(String text) {
		try {
			return UUID.nameUUIDFromBytes(hash(text, "SHA-1"));
		} catch (NoSuchAlgorithmException ex) {
			Logger.getLogger(Hashing.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	// This is about making a UUID from two UUIDs
	public static UUID combineUUID(UUID a, UUID b) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		md.update(UUIDToBytes(a));
		md.update(UUIDToBytes(b));
		return UUID.nameUUIDFromBytes(md.digest());
	}

	private static byte[] UUIDToBytes(UUID u) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(u.getMostSignificantBits());
		bb.putLong(u.getLeastSignificantBits());
		return bb.array();
	}
}
