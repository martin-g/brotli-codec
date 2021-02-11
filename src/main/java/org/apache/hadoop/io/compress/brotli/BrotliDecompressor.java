/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.hadoop.io.compress.brotli;

import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BrotliDecompressor implements Decompressor {

  private static final Logger LOG = LoggerFactory.getLogger(BrotliDecompressor.class);

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private final ByteArrayOutputStream outBuffer;
  private final StackTraceElement[] stack;

  private BrotliInputStream inBuffer = null;
  private long totalBytesIn = 0;
  private long totalBytesOut = 0;

  public BrotliDecompressor() {
    this.outBuffer = new ByteArrayOutputStream();
    this.stack = Thread.currentThread().getStackTrace();
  }

  private boolean hasMoreOutput() {
    return !isOutputBufferEmpty() /*|| decompressor.needsMoreOutput()*/;
  }

  private boolean hasMoreInput() {
    return (inBuffer.available() > 0);
  }

  private boolean isOutputBufferEmpty() {
    return (outBuffer.size() == 0);
  }

  private boolean isInputBufferEmpty() {
    return !hasMoreInput();
  }

  @Override
  public void setInput(byte[] inBytes, int off, int len) {
    Preconditions.checkState(isInputBufferEmpty(),
        "[BUG] Cannot call setInput with existing unconsumed input.");
    // this must use a ByteBuffer because not all of the bytes must be consumed
    this.inBuffer = new BrotliInputStream(new ByteArrayInputStream(inBytes, off, len));
    getMoreOutput();
    totalBytesIn += len;
  }

  private void getMoreOutput() {
    Preconditions.checkState(isOutputBufferEmpty(),
        "[BUG] Cannot call getMoreOutput without consuming all output.");
    outBuffer.reset();
    try {
      byte[] buf = new byte[4096];
      int len;
      while ((len = inBuffer.read(buf)) != 0) {
        outBuffer.write(buf, 0, len);
      }
    } catch (IOException iox) {
      LOG.error("An error occurred while decompressing", iox);
    }
  }

  @Override
  public boolean needsInput() {
    return isInputBufferEmpty() /*&& inBuffer.needsMoreInput()*/ && !hasMoreOutput();
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException("Brotli decompression does not support dictionaries");
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean finished() {
    return isInputBufferEmpty() && /*!decompressor.needsMoreInput() &&*/ !hasMoreOutput();
  }

  @Override
  public int decompress(byte[] out, int off, int len) throws IOException {
    int bytesCopied = 0;
    int currentOffset = off;

    if (isOutputBufferEmpty() && (hasMoreInput() || decompressor.needsMoreOutput())) {
      getMoreOutput();
    }

    while (bytesCopied < len && hasMoreOutput()) {
      int bytesToCopy = Math.min(len - bytesCopied, outBuffer.remaining());
      outBuffer.get(out, currentOffset, bytesToCopy);
      currentOffset += bytesToCopy;

      if (isOutputBufferEmpty() && (hasMoreInput() || decompressor.needsMoreOutput())) {
        getMoreOutput();
      }

      bytesCopied += bytesToCopy;
    }

    totalBytesOut += bytesCopied;

    return bytesCopied;
  }

  @Override
  public int getRemaining() {
    int available = outBuffer.size();
    if (available > 0) {
      return available;
    } else if (decompressor.needsMoreOutput()) {
      getMoreOutput();
      return outBuffer.size();
    } else if (decompressor.needsMoreInput()) {
      return 1;
    }
    return 0;
  }

  @Override
  public void reset() {
    Preconditions.checkState(isOutputBufferEmpty(),
        "Reused without consuming all output");
    end();
    outBuffer.reset();
    this.inBuffer = null;
    this.totalBytesIn = 0;
    this.totalBytesOut = 0;
  }

  @Override
  public void end() {
    if (!isOutputBufferEmpty()) {
      LOG.warn("Closed without consuming all output");
    }
    if (inBuffer != null) {
      try {
        inBuffer.close();
      } catch (IOException iox) {
        LOG.error("An error occurred while closing the input stream", iox);
      } finally {
        inBuffer = null;
      }
    }

    if (outBuffer != null) {
      try {
        outBuffer.close();
      } catch (IOException iox) {
        LOG.error("An error occurred while closing the output stream", iox);
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (inBuffer != null) {
      end();
      String trace = Joiner.on("\n\t").join(
          Arrays.copyOfRange(stack, 1, stack.length));
      LOG.warn("Unclosed Brotli decompression stream created by:\n\t" + trace);
    }
  }
}
