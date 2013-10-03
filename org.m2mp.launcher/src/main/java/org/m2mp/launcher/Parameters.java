/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.m2mp.launcher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author florent
 */
public final class Parameters {

	private String configFileName = "/etc/m2mp/checker.json";
	private SortedSet<String> excludedServiceNames = new TreeSet<>();
	ConfigGlobal config;
	
	public boolean isServiceExcluded( String queueName ) {
		return excludedServiceNames.contains(queueName);
	}

	public ConfigGlobal getConfig() {
		if (config == null) {
			try {
				File file = new File(configFileName);
				if (file.exists()) {
					config = getGson().fromJson(new FileReader(file), ConfigGlobal.class);
					if( config == null ) {
						config = new ConfigGlobal();
						saveConfig();
					}
				} else {
					config = new ConfigGlobal();
					saveConfig();
				}
			} catch (Exception ex) {
				Logger.getLogger(Parameters.class.getName()).log(Level.SEVERE, null, ex);
				config = new ConfigGlobal();
			}
		}
		return config;
	}
	
	public Gson getGson() {
		return new GsonBuilder().setPrettyPrinting().create();
	}

	public void saveConfig() throws IOException {
		try (FileWriter writer = new FileWriter(configFileName)) {
			getGson().toJson(getConfig(),writer);
		}
	}

	public Parameters(String[] argv) throws IOException {
		boolean save = false;
		for (int i = 0; i < argv.length; i++) {
			String arg = argv[i];
			switch (arg) {
				case "--config-file":
				case "-c":
					configFileName = argv[++i];
					config = null;
					break;
				case "--running-time":
				case "-rt":
					getConfig().runningTime = Long.parseLong(argv[++i]);
					break;
				case "--check-period":
				case "-cp":
					getConfig().checkPeriod = Long.parseLong(argv[++i]);
					break;
				case "--fail-count":
				case "-fc":
					getConfig().failCount = Integer.parseInt(argv[++i]);
					break;
				case "--delay-between-mails":
				case "-dbm":
					getConfig().delayBetweenMails = Long.parseLong(argv[++i]);
					break;
				case "--recipient":
				case "-r":
					getConfig().emails.add(argv[++i]);
					break;
				case "--add":
				case "-a":
					getConfig().services.add(new ConfigService(argv[++i], argv[++i]));
					break;
				case "--excluse":
				case "-e":
					excludedServiceNames.add(argv[++i]);
					break;
				case "--save":
				case "-s":
					save = true;
					break;
				case "--verbose":
				case "-v":
					getConfig().verbose = true;
					break;
				default:
					System.err.println("Option " + arg + " not understood!");
					break;
			}
		}

		if (getConfig().emails.isEmpty()) {
			getConfig().emails.add("florent.clairambault@gmail.com");
			save = true;
		}

		if (getConfig().services.isEmpty()) {
			getConfig().services.add(new ConfigService("m2mp_directory", "M2MP_directory/dist/M2MP_directory.jar"));
			save = true;
		}
		
		if ( config.check() )
			save = true;
		
		if ( save )
			saveConfig();
	}
}
