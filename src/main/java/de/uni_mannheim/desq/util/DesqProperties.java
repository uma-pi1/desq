package de.uni_mannheim.desq.util;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.*;

public class DesqProperties extends PropertiesConfiguration implements Externalizable {
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        try {
            write(new OutputStreamWriter(bytesOut));
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
        byte[] bytes = bytesOut.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        try {
            read(new InputStreamReader(new ByteArrayInputStream(bytes)));
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
    }
}
