package k.anand.jsonexternalsorter;

import java.io.File;
import java.io.IOException;

/**
 * @author anand-8434 on Feb 26, 2021 at 5:15:46 PM
 *
 */
public class BufferedChannelInputStreamLog extends BufferedChannelInputStream {

	/**
	 * @param file
	 * @param bufferSize
	 * @throws IOException
	 */
	public BufferedChannelInputStreamLog(File file, int bufferSize) throws IOException {
		super(file, bufferSize);
	}
	
	@Override
	public int read() throws IOException {
		int r = super.read();
		System.out.print((char)r);
		return r;
	}
	
	@Override
	public int read(byte[] bytes) throws IOException {
		int n = super.read(bytes);
		System.out.print(new String(bytes));
		return n;
	}
	
	@Override
	public int read(byte[] bytes, int start, int length) throws IOException {
		int n = super.read(bytes, start, length);
		System.out.print(new String(bytes));
		return n;
	}
}
