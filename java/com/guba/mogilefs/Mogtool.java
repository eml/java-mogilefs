package com.guba.mogilefs;

import java.io.File;

/**
 * Mogtool -- command-line tool for interacting with Mogile
 * A poor-man's copy of the perl tool.
 *  
 * @author sam
 */
public class Mogtool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 1) {
			showUsage();
			return;
		}
		final String command = args[0];
		if ("inject".equalsIgnoreCase(command)) {
			if (args.length < 4) {
				System.err.println("mogtool inject <key> <class> <filename>");
				return;
			}
			try {
				inject(args[1], args[2], args[3]);
			} catch (MogileException e) {
				System.err.println("Error trying to inject file: " + e);
				e.printStackTrace();
			}
		}
		// TODO: add other commands
		else {
			System.err.println("Unknown command: '" + command + "'");
			showUsage();
		}
	}
	
	// TODO: make this configuration not hard-coded
	protected static MogileFS createMogileFS() throws NoTrackersException, BadHostFormatException {
		MogileFS mogileFS = new PooledMogileFSImpl("creator.guba.com",
				new String[] { "tracker1.guba.com:7001", "tracker2.guba.com:7001", "tracker3.guba.com:7001", "tracker4.guba.com:7001" },
				5, 2, 30000);
		return mogileFS;
	}

	public static void inject(String key, String storageClass, String filename) throws MogileException {
		MogileFS mogileFS = createMogileFS();
		
		File file = new File(filename);
		System.out.println("storing " + file + " as " + key + " to " + mogileFS);
		mogileFS.storeFile(key, storageClass, file);
	}

	private static void showUsage() {
		System.err.println("Mogtool: <command> [args]");
		System.err.println(" where command is one of the following:");
		System.err.println("\t inject <key> <class> <filename>");
		System.err.println("\t\t  (other functionality coming soon...)");
		System.err.println();
	}

}
