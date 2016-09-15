package de.uni_mannheim.desq.util;

import com.google.common.base.Strings;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

    public void prettyPrint() {
        prettyPrint(System.out, 2);
    }

    public void prettyPrint(PrintStream out, int indent) {
        String indentString = Strings.repeat(" ", indent);
        List<String> keys = new ArrayList<>();
        getKeys().forEachRemaining(keys::add);
        Collections.sort(keys);
        for (String key : keys) {
            out.print(indentString);
            out.print(key);
            out.print("=");
            out.print(getString(key));
            out.println();
        }
    }
}
