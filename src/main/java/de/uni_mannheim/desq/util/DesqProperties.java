package de.uni_mannheim.desq.util;

import com.google.common.base.Strings;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.*;

/** Light-weight collection of properties */
public class DesqProperties implements Externalizable, Writable {
    private HashMap<String,String> properties;

    public DesqProperties() {
        properties = new HashMap<>(0);
    }

    public DesqProperties(int initialCapacity) {
        properties = new HashMap<>(initialCapacity);
    }

    public DesqProperties(Properties prop) {
        properties = new HashMap<>(prop.size());
        for(Map.Entry<Object,Object> entry : prop.entrySet()) {
            setProperty((String)entry.getKey(), entry.getValue());
        }
    }

    public DesqProperties(DesqProperties prop) {
        properties = new HashMap<>(prop.size());
        for(Map.Entry<String,String> entry : prop.properties.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value.toString());
    }

    public String getString(String key) {
        String value = properties.get(key);
        if (value != null) {
            return value;
        } else if (properties.containsKey(key)) {
            return null;
        } else {
            throw new NoSuchElementException(String.format(
                "Key '%s' does not map to an existing object!", key));
        }
    }

    public String getString(String key, String defaultValue) {
        String value = properties.get(key);
        if (value != null) {
            return value;
        } else if (properties.containsKey(key)) {
            return null;
        } else {
            return defaultValue;
        }
    }

    public int getInt(String key) {
        String value = getString(key);
        return Integer.parseInt(value);
    }

    public long getLong(String key) {
        String value = getString(key);
        return Long.parseLong(value);
    }

    public boolean getBoolean(String key) {
        String value = getString(key);
        return Boolean.parseBoolean(value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null && !properties.containsKey(key)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    public Properties toProperties() {
        Properties result = new Properties();
        for (Map.Entry<String, String> p : properties.entrySet()) {
            result.put(p.getKey(), p.getValue());
        }
        return result;
    }

    public Iterator<String> getKeys() {
        return properties.keySet().iterator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFields(in);
    }

    public void prettyPrint() {
        prettyPrint(System.out, 2);
    }

    public void prettyPrint(PrintStream out, int indent) {
        String indentString = Strings.repeat(" ", indent);
        List<String> keys = new ArrayList<>();
        properties.keySet().forEach(keys::add);
        Collections.sort(keys);
        for (String key : keys) {
            out.print(indentString);
            out.print(key);
            out.print("=");
            out.print(getString(key));
            out.println();
        }
    }

    public int size() {
        return properties.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, properties.size());
        for (Map.Entry<String, String> p : properties.entrySet()) {
            out.writeUTF(p.getKey());
            out.writeUTF(p.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int size = WritableUtils.readVInt(in); size>0; size--) {
            String key = in.readUTF();
            String value = in.readUTF();
            properties.put(key, value);
        }
    }
}
