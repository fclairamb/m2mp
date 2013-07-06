/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.db.test;

import org.m2mp.db.Shared;

/**
 *
 * @author florent
 */
public class BaseTest {

	public static void setUpClass() {
		Shared.switchToTest();
	}
}
