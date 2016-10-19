package de.uni_mannheim.desq.util;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by rgemulla on 14.10.2016.
 */
public final class DataInput2InputStreamWrapper extends InputStream {
    public DataInput in;
    public int remainingBytes;

    public DataInput2InputStreamWrapper(DataInput in, int maxBytesToRead) {
        this.in = in;
        this.remainingBytes = maxBytesToRead;
    }

    @Override
    public int read() throws IOException {
        remainingBytes--;
        try {
            return (remainingBytes >= 0) ? in.readUnsignedByte() : -1;
        } catch (EOFException e) {
            remainingBytes = 0;
            return -1;
        }
    }
}
