package org.apache.hadoop.io.compress.brotli;

import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
class BrotliDirectDecompressorTest {

	@Test
	void decompress() throws IOException {
		String input = "Hello Brotli";

		BrotliCodec codec = new BrotliCodec();

		Compressor compressor = codec.createCompressor();

		byte[] uncompressed = input.getBytes(StandardCharsets.UTF_8);
		compressor.setInput(uncompressed, 0, uncompressed.length);
		compressor.finish();

		byte[] compressed = new byte[/*(int) compressor.getBytesWritten()*/ 16];
		final int compressedBytes = compressor.compress(compressed, 0, compressed.length);
		assertEquals(compressed.length, compressedBytes);


		ByteBuffer src = ByteBuffer.wrap(compressed);
		ByteBuffer dest = ByteBuffer.allocateDirect(16);
		BrotliDirectDecompressor decompressor = new BrotliDirectDecompressor();
		decompressor.decompress(src, dest);

		assertFalse(src.hasRemaining());

		dest.flip();
		byte[] result = new byte[input.length()];
		dest.get(result, 0, result.length);
		assertEquals(input, new String(result));
	}
}
