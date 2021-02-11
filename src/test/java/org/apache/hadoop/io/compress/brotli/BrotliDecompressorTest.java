package org.apache.hadoop.io.compress.brotli;

import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
class BrotliDecompressorTest {

	@Test
	void decompress() throws IOException {
		BrotliCodec codec = new BrotliCodec();

		Compressor compressor = codec.createCompressor();

		byte[] uncompressed = "Hello Brotli".getBytes(StandardCharsets.UTF_8);
		compressor.setInput(uncompressed, 0, uncompressed.length);
		compressor.finish();

		byte[] compressed = new byte[/*(int) compressor.getBytesWritten()*/ 16];
		final int compressedBytes = compressor.compress(compressed, 0, compressed.length);
		assertEquals(compressed.length, compressedBytes);

		Decompressor decompressor = codec.createDecompressor();
		decompressor.setInput(compressed, 0, compressed.length);

		byte[] result = new byte[uncompressed.length];
		final int decompressedBytes = decompressor.decompress(result, 0, result.length);
		assertEquals(uncompressed.length, decompressedBytes);
		assertArrayEquals(result, uncompressed, "=== Error");
	}
}
