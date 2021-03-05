package k.anand.jsonexternalsorter;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.json.JSONObject;

import k.anand.jsonexternalsorter.JSONIOStackBuilder.JSONArrayIOStack;
import k.anand.jsonexternalsorter.JSONIOStackBuilder.JSONIOStackBuildError;

/**
 * @author anand-8434 on Feb 25, 2021 at 12:21:40 PM
 *
 */
public class JSONExternalSort {
	public static final int DEFAULTMAXTEMPFILES = 1024;

	/**
	 * @param ioStacks
	 * @param jsonObjectComparator
	 * @throws IOException
	 */
	public static long mergeFiles(List<JSONArrayIOStack> ioStacks, Comparator<JSONObject> jsonObjectComparator, BufferedWriter sortedBufferedWriter) throws IOException {
		PriorityQueue<JSONArrayIOStack> pq = new PriorityQueue<>(11, new Comparator<JSONArrayIOStack>() {
			@Override
			public int compare(JSONArrayIOStack i, JSONArrayIOStack j) {
				return jsonObjectComparator.compare(i.peek(), j.peek());
			}
		});

		for (JSONArrayIOStack bfb : ioStacks) {
			if (!bfb.empty()) {
				pq.add(bfb);
			}
		}
		long rowcounter = 0;
		try {
			sortedBufferedWriter.write('[');
			while (pq.size() > 0) {
				JSONArrayIOStack bfb = pq.poll();
				JSONObject r = bfb.pop();
				if (rowcounter != 0) {
					sortedBufferedWriter.write(',');
					sortedBufferedWriter.newLine();
				}
				r.write(sortedBufferedWriter);
				++rowcounter;
				if (bfb.empty()) {
					bfb.close();
				} else {
					pq.add(bfb); // add it back
				}
			}
			sortedBufferedWriter.write(']');
		} finally {
			sortedBufferedWriter.close();
			for (JSONArrayIOStack bfb : pq) {
				if(bfb!= null) {
				bfb.close();
				}
			}
		}
		return rowcounter;
	}

	/**
	 * @param jsonioStackBuilder
	 * @param listOfFiles
	 * @param jsonObjectComparator
	 * @param sortedOutputFile
	 * @return
	 * @throws IOException
	 * @throws JSONIOStackBuildError
	 */
	public static List<JSONArrayIOStack> sortInBatch(JSONIOStackBuilder jsonioStackBuilder, File[] listOfFiles, Comparator<JSONObject> jsonObjectComparator) throws IOException, JSONIOStackBuildError {
		// JSONArrayIOStack jsonArrayIOStack = jsonioStackBuilder.buildJSONArrayIOStack(listOfFiles);
		List<File> batchFiles = new LinkedList<File>();
		for (File file : listOfFiles) {
			if(file.isFile()) {
				JSONArrayIOStack jsonArrayIOStack = jsonioStackBuilder.buildJSONArrayIOStack(new BufferedChannelInputStream(file, 100*1024*1024), 407983);
				batchFiles.addAll(sortInBatch(jsonArrayIOStack, file.length(), jsonObjectComparator, DEFAULTMAXTEMPFILES, estimateAvailableMemory(), Charset.defaultCharset(), null));
			}
		}
		List<JSONArrayIOStack> ioStacks = new ArrayList<JSONIOStackBuilder.JSONArrayIOStack>();
		for (File file : batchFiles) {
			if (file.isFile()) {
				ioStacks.add(jsonioStackBuilder.new JSONArrayIOStack(new BufferedChannelInputStream(file, 10*1024*1024)));
			}
		}
		return ioStacks;
	}

	public static List<File> sortInBatch(final JSONArrayIOStack jsonArrayIOStack, final long datalength, final Comparator<JSONObject> jsonObjectComparator, final int maxtmpfiles, long maxMemory, final Charset cs, final File tmpdirectory) throws IOException {
		List<File> files = new ArrayList<>();
		long blocksize = 50*1024*1024;//estimateBestSizeOfBlocks(datalength, maxtmpfiles, maxMemory);// in
		// bytes

		try {
			LinkedList<JSONObject> tmpJSONList = new LinkedList<>();;//jsonArrayIOStack.getAsBatch(blocksize);
			JSONObject jsonObject = jsonArrayIOStack.pop();
			try {
				while (jsonObject != null) {
					jsonArrayIOStack.resetCount();
					while ((jsonArrayIOStack.readCount() < blocksize) && ((jsonObject = jsonArrayIOStack.pop()) != null)) {
						tmpJSONList.add(jsonObject);
					}
					files.add(sortAndSave(tmpJSONList, jsonObjectComparator, cs, tmpdirectory));
					tmpJSONList.clear();
				}
			} catch (EOFException oef) {
				if (tmpJSONList.size() > 0) {
					files.add(sortAndSave(tmpJSONList, jsonObjectComparator, cs, tmpdirectory));
					tmpJSONList.clear();
				}
			}
		} finally {
			jsonArrayIOStack.close();
		}
		return files;
	}

	public static File sortAndSave(LinkedList<JSONObject> tmpJSONList, Comparator<JSONObject> jsonObjectComparator, Charset cs, File tmpdirectory) throws IOException {
		File sortInBatch = new File(tmpdirectory == null ? new File(System.getProperty("java.io.tmpdir")):tmpdirectory, "sortinbatch");
		if(!sortInBatch.exists()) {
			sortInBatch.mkdir();
		}
		File newtmpfile = File.createTempFile("sortInBatch", "flatfile", sortInBatch);
		newtmpfile.deleteOnExit();
		Collections.sort(tmpJSONList, jsonObjectComparator);
		try (BufferedWriter bw = Files.newBufferedWriter(newtmpfile.toPath())) {
			bw.write('[');
			while(tmpJSONList.size() > 1) {
				JSONObject r = tmpJSONList.poll();
				r.write(bw);
				bw.write(',');
				bw.newLine();
			}
			tmpJSONList.poll().write(bw);
			bw.write(']');
		}
		return newtmpfile;
	}

	public static long estimateAvailableMemory() {
		System.gc();
		// http://stackoverflow.com/questions/12807797/java-get-available-memory
		Runtime r = Runtime.getRuntime();
		long allocatedMemory = r.totalMemory() - r.freeMemory();
		long presFreeMemory = r.maxMemory() - allocatedMemory;
		return presFreeMemory;
	}

	public static long estimateBestSizeOfBlocks(final long sizeoffile, final int maxtmpfiles, final long maxMemory) {
		// we don't want to open up much more than maxtmpfiles temporary
		// files, better run
		// out of memory first.
		long blocksize = sizeoffile / maxtmpfiles + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);

		// on the other hand, we don't want to create many temporary
		// files
		// for naught. If blocksize is smaller than half the free
		// memory, grow it.
		if (blocksize < maxMemory / 2) {
			blocksize = maxMemory / 2;
		}
		return blocksize;
	}

	/**
	 * @param sortInBatch
	 * @param jsonObjectComparator
	 * @param sortedOutputFile
	 * @return
	 * @throws IOException
	 */
	public static long mergeFiles(List<JSONArrayIOStack> sortInBatch, Comparator<JSONObject> jsonObjectComparator, File sortedOutputFile) throws IOException {
		return mergeFiles(sortInBatch, jsonObjectComparator, new BufferedWriter(new FileWriter(sortedOutputFile)));
	}
}
