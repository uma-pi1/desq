package de.uni_mannheim.desq.util;

import com.esotericsoftware.kryo.io.Input;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.IOException;

/** Wraps a DataInput around a Input. */
public final class Input2DataInputWrapper implements DataInput {
    public Input input;

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readBytes(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readBytes(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readByteUnsigned();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readShortUnsigned();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public String readUTF() throws IOException {
        throw new NotImplementedException();
    }
}
