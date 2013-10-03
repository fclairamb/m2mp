/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.launcher;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author florent
 */
public class ConfigGlobal {

	private static final int DEFAULT_RUNNING_TIME = 120;
	private static final int DEFAULT_CHECK_PERIOD = 20;
	private static final int DEFAULT_FAIL_COUNT = 3;
	private static final int DEFAULT_DELAY_BETWEEN_MAILS = 300;
	private static final String DEFAULT_CHECKER_DIRECTORY = "checker";
	private static final String DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-7-oracle";
	public String javahome = DEFAULT_JAVA_HOME;
	public long runningTime = DEFAULT_RUNNING_TIME;
	public long checkPeriod = DEFAULT_CHECK_PERIOD;
	public int failCount = DEFAULT_FAIL_COUNT;
	public long delayBetweenMails = DEFAULT_DELAY_BETWEEN_MAILS;
	public boolean verbose;
	public String checkerDirectory = DEFAULT_CHECKER_DIRECTORY;
	public Collection<ConfigService> services = new ArrayList<>();
	public Collection<String> emails = new ArrayList<>();

	public boolean check() {
		boolean r = false;
		for (ConfigService s : services) {
			if (s.check()) {
				r = true;
			}
		}
		return r;
	}
}
