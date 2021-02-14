package org.apache.hadoop.io.compress.brotli;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrotliDecompressorTest {

	@Test
	void decompress() throws IOException {
		BrotliCodec codec = new BrotliCodec();

		Compressor compressor = codec.createCompressor();

		InputStream textStream = BrotliDirectDecompressorTest.class.getResourceAsStream("/Words.txt");
		byte[] textBytes = new byte[textStream.available()];
		IOUtils.readFully(textStream, textBytes, 0, textBytes.length);

		compressor.setInput(textBytes, 0, textBytes.length);
		compressor.finish();

		byte[] compressed = new byte[textBytes.length];
		final int compressedBytes = compressor.compress(compressed, 0, compressed.length);
		compressor.end();
		assertTrue(compressedBytes < textBytes.length, "Compressed size must be smaller than the input size");

		Decompressor decompressor = codec.createDecompressor();
		decompressor.setInput(compressed, 0, compressedBytes);

		byte[] result = new byte[textBytes.length];
		final int decompressedBytes = decompressor.decompress(result, 0, result.length);
		decompressor.end();

		assertEquals(textBytes.length, decompressedBytes);
		assertEquals(new String(textBytes), new String(result));
	}
}
