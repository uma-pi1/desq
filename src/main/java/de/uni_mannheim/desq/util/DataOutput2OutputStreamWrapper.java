package de.uni_mannheim.desq.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by rgemulla on 05.10.2016.
 */
public class DataOutput2OutputStreamWrapper extends OutputStream {
    public DataOutput dataOutput;

    public DataOutput2OutputStreamWrapper(DataOutput dataOutput) {
        this.dataOutput = dataOutput;
    }

    @Override
    public void write(int b) throws IOException {
        dataOutput.write(b);
    }
}
