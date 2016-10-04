package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.hadoop.io.WritableUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/** A sequence of integers. */
public class Sequence extends IntArrayList implements Externalizable {
    public Sequence() {
        super();
    }

    protected Sequence(int capacity) {
        super(capacity);
    }

    public Sequence(final IntList l) {
        super(l);
    }

    @Override
    public Sequence clone() {
        Sequence c = new Sequence(size);
        System.arraycopy(this.a, 0, c.a, 0, this.size);
        c.size = this.size;
        return c;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        WritableUtils.writeVInt(out, size);
        for (int i=0; i<size; i++) {
            WritableUtils.writeVInt(out, a[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size(WritableUtils.readVInt(in));
        for (int i=0; i<size; i++) {
            a[i] = WritableUtils.readVInt(in);
        }
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i=0; i<size(); i++)
            result = 31 * result + a[i];
        return result;
    }
}
