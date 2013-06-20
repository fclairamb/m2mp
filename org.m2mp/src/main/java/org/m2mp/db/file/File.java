/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.file;

import com.datastax.driver.core.Session;
import java.util.UUID;

/**
 * This class should be used for one session.
 *
 * @author Florent Clairambault
 */
public class File {

	public File(Session session, String path) {
	}

	public File(Session session, UUID id) {
	}
}
