package de.uni_mannheim.desq.util;

import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataOutput;
import java.io.IOException;

/** Wraps a DataOutput around an Output. */
public class Output2DataOutputWrapper implements DataOutput {
    public Output output;

    @Override
    public void write(int b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        output.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        output.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        output.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        output.writeChar((char)v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        output.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        output.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        output.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        output.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new NotImplementedException();
    }
}
