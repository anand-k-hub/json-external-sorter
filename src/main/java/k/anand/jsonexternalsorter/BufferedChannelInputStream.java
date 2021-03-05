package k.anand.jsonexternalsorter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;

import sun.nio.ch.ChannelInputStream;

/**
 * @author anand-8434 on Feb 26, 2021 at 3:37:39 PM
 *
 */
public class BufferedChannelInputStream extends InputStream {
	private ReadableByteChannel bc;
	private ByteBuffer byteBuffer;

	/**
	 * @throws IOException
	 * 
	 */
	public BufferedChannelInputStream(File file, int bufferSize) throws IOException {
		bc = Files.newByteChannel(file.toPath());
		this.byteBuffer = ByteBuffer.allocate(bufferSize);
		ChannelInputStream.read(bc, byteBuffer, true);
		byteBuffer.flip();
	}

	@Override
	public int read() throws IOException {
		if (!fillIfEmpty()) {
			return -1;
		}
		return byteBuffer.get();
	}

	@Override
	public int read(byte[] bytes, int start, int length) throws IOException {
		if (!fillIfEmpty()) {
			return -1;
		}
		return readByOffset(bytes, start, length);
	}

	private int readByOffset(byte[] bytes, int start, int length) throws IOException {
		if (byteBuffer.hasRemaining() && byteBuffer.remaining() < length) {
			int partial = byteBuffer.remaining();
			byteBuffer.get(bytes, start, start + partial);
			if (available() == 0) {
				return partial;
			}
			byteBuffer.clear();
			ChannelInputStream.read(bc, byteBuffer, true);
			byteBuffer.flip();
			return readByOffset(bytes, start + partial, length - partial) + partial;
		}
		int position = byteBuffer.position();
		return byteBuffer.get(bytes, start, length).position() - position;
	}

	@Override
	public int read(byte[] bytes) throws IOException {
		if (!fillIfEmpty()) {
			return -1;
		}
		if (byteBuffer.hasRemaining() && byteBuffer.remaining() < bytes.length) {
			int partial = byteBuffer.remaining();
			byteBuffer.get(bytes, 0, partial);
			if (available() == 0) {
				return partial;
			}
			byteBuffer.clear();
			ChannelInputStream.read(bc, byteBuffer, true);
			byteBuffer.flip();
			return readByOffset(bytes, partial, bytes.length - partial) + partial;
		}
		int position = byteBuffer.position();
		return byteBuffer.get(bytes).position() - position;
	}

	private boolean fillIfEmpty() throws IOException {
		if (!byteBuffer.hasRemaining()) {
			if (available() == 0) {
				return false;
			}
			byteBuffer.clear();
			ChannelInputStream.read(bc, byteBuffer, true);
			byteBuffer.flip();
		}
		return true;
	}

	@Override
	public void close() throws IOException {
		bc.close();
	}

	@Override
	public int available() throws IOException {
		SeekableByteChannel sbc = (SeekableByteChannel) bc;
		long rem = Math.max(0, sbc.size() - sbc.position());
		return (rem > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) rem;
	}
}
