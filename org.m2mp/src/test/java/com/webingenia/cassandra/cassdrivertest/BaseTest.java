/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.webingenia.cassandra.cassdrivertest;

import org.junit.BeforeClass;
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
