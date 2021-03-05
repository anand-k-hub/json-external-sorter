package k.anand.jsonexternalsorter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * @author anand-8434 on Feb 24, 2021 at 7:53:27 PM
 *
 */
public class JSONIOStackBuilder {

	private File storeDir = new File(System.getProperty("java.io.tmpdir"));
	private long maximumFileCount = 1000; // TODO have to check https://stackoverflow.com/questions/8238860/maximum-number-of-files-directories-on-linux

	public JSONIOStackBuilder storeDir(String storeDir) {
		return storeDir(new File(storeDir));
	}

	public JSONIOStackBuilder storeDir(File storeDir) {
		this.storeDir = storeDir;
		return this;
	}

	public JSONIOStackBuilder maximumFileCount(int count) {
		this.maximumFileCount = count;
		return this;
	}

	/**
	 * @param cacheCount
	 * @param zohoLogJSONStreamFilter
	 * @return
	 * @throws IOException
	 */
	public JSONArrayIOStack buildJSONArrayIOStack(InputStream inputStream, int cacheCount) throws IOException {
		return new JSONArrayIOStack(new LengthStream(inputStream), cacheCount);
	}

	public JSONArrayIOStack buildJSONArrayIOStack(File file) throws JSONIOStackBuildError {
		try {
			return new JSONArrayIOStack(file);
		} catch (IOException e) {
			throw new JSONIOStackBuildError("Build creation was failed for JSONArrayIOStack");
		}
	}

	/**
	 * @param listOfFiles
	 * @return
	 * @throws IOException
	 */
	public JSONArrayIOStack buildJSONArrayIOStack(File[] listOfFiles) throws IOException {
		Vector<InputStream> v = new Vector<>(listOfFiles.length);
		for (File file : listOfFiles) {
			if (file.isFile()) {
				v.add(new BufferedInputStream(new FileInputStream(file)));
			}
		}
		LengthStream sis = new LengthStream(new SequenceInputStream(v.elements()));
		return new JSONArrayIOStack(sis);
	}

	class LengthStream extends FilterInputStream {
		/**
		 * @param in
		 */
		protected LengthStream(InputStream in) {
			super(in);
		}

		long size = 0;

		@Override
		public int read() throws IOException {
			size++;
			return super.read();
		}

		@Override
		public int read(byte[] b) throws IOException {
			int n = super.read(b);
			size += n;
			return n;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int n = super.read(b, off, len);
			size += n;
			return n;
		}
	}

	public class JSONArrayIOStack implements IOStack<JSONObject>, Runnable {
		private ArrayBlockingQueue<JSONObject> cache;
		private JSONTokener jsonTokener;
		private LengthStream is;
		private boolean isFinished;

		/**
		 * @param file
		 * @throws IOException
		 */
		public JSONArrayIOStack(File file) throws IOException {
			this(file, 1);
		}

		/**
		 * @param file
		 * @throws IOException
		 */
		public JSONArrayIOStack(File file, int cacheSize) throws IOException {
			this(new LengthStream(new BufferedInputStream(new FileInputStream(file))), cacheSize);
		}

		/**
		 * @param is
		 * @throws IOException
		 * 
		 */
		public JSONArrayIOStack(InputStream is) throws IOException {
			this(new LengthStream(is), 1);
		}

		/**
		 * @param is
		 * @throws IOException
		 * 
		 */
		public JSONArrayIOStack(LengthStream is, int cacheSize) throws IOException {
			this.cache = new ArrayBlockingQueue<JSONObject>(cacheSize);
			this.is = is;
			this.jsonTokener = new JSONTokener(is);
			// initial validation
			if (jsonTokener.nextClean() != '[') {
				throw jsonTokener.syntaxError("A JSONArray text must start with '['");
			}

			char nextChar = jsonTokener.nextClean();
			if (nextChar == 0) {
				// array is unclosed. No ']' found, instead EOF
				throw jsonTokener.syntaxError("Expected a ',' or ']'");
			}
			jsonTokener.back();
			new Thread(this).start();
		}

		@Override
		public boolean empty() {
			return peek == null && this.cache.isEmpty() && isFinished;
		}

		private JSONObject peek = null;

		@Override
		public JSONObject peek() {
			if (peek == null) {
				peek = pop();
			}
			return peek;
		}

		@Override
		public JSONObject pop() {
			JSONObject r = null;
			if (peek != null) {
				r = peek;
				peek = null;
			}
			while (r == null && !empty()) {
				try {
					r = cache.poll(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return r;
		}

		@Override
		public void run() {
			try {
				JSONObject jsonObj = readNextJSONObj();
				while (jsonObj != null) {
					this.cache.put(jsonObj);
					jsonObj = readNextJSONObj();
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				isFinished = true;
			}
		}

		private JSONObject readNextJSONObj() {
			if (jsonTokener.end()) {
				return null;
			}
			JSONObject r = null;
			char nextChar = jsonTokener.nextClean();
			if (nextChar != ']' && nextChar != 0) {
				jsonTokener.back();
				if (jsonTokener.nextClean() == ',') {
					jsonTokener.back();
				} else {
					jsonTokener.back();
					r = (JSONObject) jsonTokener.nextValue();
				}
				switch (jsonTokener.nextClean()) {
					case 0:
						// array is unclosed. No ']' found, instead EOF
						throw jsonTokener.syntaxError("Expected a ',' or ']'");
					case ',':
						nextChar = jsonTokener.nextClean();
						if (nextChar == 0) {
							// array is unclosed. No ']' found, instead EOF
							throw jsonTokener.syntaxError("Expected a ',' or ']'");
						}
						if (nextChar == ']') {
							return r;
						}
						jsonTokener.back();
						break;
					case ']':
						jsonTokener.back();
						return r;
					default:
						throw jsonTokener.syntaxError("Expected a ',' or ']'");
				}
			}
			return r;
		}

		@Override
		public void close() throws IOException {
			is.close();
		}

		public long readCount() {
			return is.size;
		}

		public void resetCount() {
			is.size = 0;
		}

		/**
		 * @param blocksize
		 * @return
		 */
		public List<JSONObject> getAsBatch(long blocksize) {
			return null;
		}
	}

	class JSONIOStackBuildError extends Exception {
		public JSONIOStackBuildError(String message) {
			super(message);
		}
	}
}
