package com.bazaarvoice.emodb.blob.client;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

class LimitedInputStream extends FilterInputStream {

    private long left;
    private long mark = -1;

    LimitedInputStream(InputStream in, long limit) {
        super(in);
        requireNonNull(in);
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        left = limit;
    }

    @Override public int available() throws IOException {
        return (int) Math.min(in.available(), left);
    }

    // it's okay to mark even if mark isn't supported, as reset won't work
    @Override public synchronized void mark(int readLimit) {
        in.mark(readLimit);
        mark = left;
    }

    @Override public int read() throws IOException {
        if (left == 0) {
            return -1;
        }

        int result = in.read();
        if (result != -1) {
            --left;
        }
        return result;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
        if (left == 0) {
            return -1;
        }

        len = (int) Math.min(len, left);
        int result = in.read(b, off, len);
        if (result != -1) {
            left -= result;
        }
        return result;
    }

    @Override public synchronized void reset() throws IOException {
        if (!in.markSupported()) {
            throw new IOException("Mark not supported");
        }
        if (mark == -1) {
            throw new IOException("Mark not set");
        }

        in.reset();
        left = mark;
    }

    @Override public long skip(long n) throws IOException {
        n = Math.min(n, left);
        long skipped = in.skip(n);
        left -= skipped;
        return skipped;
    }
}