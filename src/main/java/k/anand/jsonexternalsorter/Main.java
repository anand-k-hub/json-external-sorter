package k.anand.jsonexternalsorter;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONObject;

import k.anand.jsonexternalsorter.JSONIOStackBuilder.JSONIOStackBuildError;

/**
 * @author anand-8434 on Feb 25, 2021 at 12:19:41 PM
 *
 */
public class Main {
	private static final char EXTENSION_SEPARATOR = '.'; // No I18N

	/**
	 * 
	 * @param args
	 * @throws JSONIOStackBuildError
	 * @throws IOException
	 * @throws Exception
	 */
	public static void main(String[] args) throws IOException, JSONIOStackBuildError {
		Options options = new Options();
		options.addOption("f", "file", true, "unsorted json file");
		options.addOption("d", "directory", true, "unsorted json files directory");
		options.addOption("o", "output", true, "sorted json file name");

		CommandLineParser parser = new DefaultParser();
		File[] listOfFiles = null;
		File output = null;
		try {
			CommandLine cmd = parser.parse(options, args);
			if (cmd.hasOption("f")) {
				File file = new File(cmd.getOptionValue("f"));
				if (file.exists()) {
					listOfFiles = new File[] { file };
				} else {
					throw new IllegalArgumentException("Given file not exisits: " + file);
				}
			} else if (cmd.hasOption("d")) {
				File directory = new File(cmd.getOptionValue("d"));
				if (directory.isDirectory()) {
					listOfFiles = directory.listFiles();
				} else {
					throw new IllegalArgumentException("Given directory is not a directory: " + directory);
				}
			} else {
				throw new IllegalArgumentException("Either " + options.getOption("f") + " or " + options.getOption("d") + " option should have to pass");
			}

			if (cmd.hasOption("o")) {
				output = new File(cmd.getOptionValue("o"));
				output.deleteOnExit();
			} else {
				File outputdir = new File(listOfFiles[0].getParent(), "sorted_json");
				outputdir.mkdir();
				output = new File(outputdir, "sorted.json");
			}
		} catch (ParseException pe) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("JSON External Sorter", options);// No I18N
			return;
		}

		JSONIOStackBuilder jsonioStackBuilder = new JSONIOStackBuilder();
		Comparator<JSONObject> jsonObjectComparator = new Comparator<JSONObject>() {
			@Override
			public int compare(JSONObject o1, JSONObject o2) {
				Date time1 = new Date(o1.getLong("_zl_timestamp"));
				Date time2 = new Date(o2.getLong("_zl_timestamp"));
				return time1.compareTo(time2);
			}
		};
		// toJSON(listOfFiles, jsonDir);
		// listOfFiles = jsonDir.listFiles();
		// List<JSONArrayIOStack> ioStacks = new ArrayList<JSONIOStackBuilder.JSONArrayIOStack>();
		// for (File file : directory.listFiles()) {
		// if (file.isFile()) {
		// ioStacks.add(jsonioStackBuilder.new JSONArrayIOStack(new BufferedChannelInputStream(file, 10*1024*1024)));
		// }
		// }
		// long count = JSONExternalSort.mergeFiles(ioStacks, jsonObjectComparator, sortedOutputFile);
		long count = JSONExternalSort.mergeFiles(JSONExternalSort.sortInBatch(jsonioStackBuilder, listOfFiles, jsonObjectComparator), jsonObjectComparator, output);
		System.out.println("No of sorted jsons: " + count);
	}

	static String removeExtension(String filename) {
		if (filename == null) {
			return null;
		}
		final int index = filename.lastIndexOf(EXTENSION_SEPARATOR);

		if (index == -1) {
			return filename;
		} else {
			return filename.substring(0, index);
		}
	}
}
