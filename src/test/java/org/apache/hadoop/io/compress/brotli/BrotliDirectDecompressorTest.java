package org.apache.hadoop.io.compress.brotli;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	@Test
	void decompress_longText() throws IOException {
		final InputStream textStream = BrotliDirectDecompressorTest.class.getResourceAsStream("/Words.txt");
		byte[] textBytes = new byte[textStream.available()];
		IOUtils.readFully(textStream, textBytes, 0, textBytes.length);
		String input = new String(textBytes);

		BrotliCodec codec = new BrotliCodec();

		Compressor compressor = codec.createCompressor();

		byte[] uncompressed = input.getBytes(StandardCharsets.UTF_8);
		compressor.setInput(uncompressed, 0, uncompressed.length);
		compressor.finish();

		byte[] compressed = new byte[textBytes.length];
		int compressedBytes = compressor.compress(compressed, 0, compressed.length);
		compressor.end();
		assertTrue(compressedBytes < textBytes.length, "Compressed size must be smaller than the input size");

		ByteBuffer src = ByteBuffer.wrap(compressed, 0 , compressedBytes);
		ByteBuffer dest = ByteBuffer.allocateDirect(textBytes.length);
		BrotliDirectDecompressor decompressor = new BrotliDirectDecompressor();
		decompressor.decompress(src, dest);

		assertFalse(src.hasRemaining(), "There is more input to decompress");

		dest.flip();
		byte[] result = new byte[textBytes.length];
		dest.get(result, 0, result.length);
		assertEquals(input, new String(result));
	}
}
