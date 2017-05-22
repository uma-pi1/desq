package de.uni_mannheim.desq.mining;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.util.List;

/**
 * Created by ivo on 16.05.17.
 */
public final class IdentifiableWeightedSequence extends WeightedSequence implements Externalizable, Writable {

    public long id;

    public IdentifiableWeightedSequence() {
        super();
        id = -1;
    }

    public IdentifiableWeightedSequence(long id, long support) {
        super(support);
        this.id = id;
    }

    public IdentifiableWeightedSequence(long id, IntList l, long support) {
        super(l, support);
        this.id = id;
    }

    protected IdentifiableWeightedSequence(long id, int capacity) {
        super(capacity);
        this.id = id;
    }

    public IdentifiableWeightedSequence(long id, final int[] a, long support) {
        super(a, support);
        this.id = id;
    }

    public String getUniqueIdentifier() {
        String uid = "";
        for (int i = 0; i < this.a.length; i++) {
            if (this.a[i] > 99) {
                uid += String.valueOf(this.a[i]);
            } else if (this.a[i] > 9) {
                uid += "0" + String.valueOf(this.a[i]);
            } else {
                uid += "00" + String.valueOf(this.a[i]);
            }
        }
        return uid;
    }

        @Override
        public IdentifiableWeightedSequence clone () {
            IdentifiableWeightedSequence c = new IdentifiableWeightedSequence(id, size);
            System.arraycopy(this.a, 0, c.a, 0, this.size);
            c.size = this.size;
            c.weight = weight;
            c.id = this.id;
            return c;
        }

        /** Returns a weigthed sequence with the same content and the specified support. Data copying is avoided to
         * the extend possible, i.e., the returned sequence may share date with this sequence. */
        @Override
        public IdentifiableWeightedSequence withSupport ( long support){
            if (this.weight == support) return this;
            IdentifiableWeightedSequence result = new IdentifiableWeightedSequence(id, a, support);
            result.size = this.size;
            return result;
        }

        @Override
        public int compareTo (List < ? extends Integer > o){
            if (o instanceof IdentifiableWeightedSequence) {
                int cmp = Long.signum(((IdentifiableWeightedSequence) o).weight - weight); // descending
                if (cmp != 0)
                    return cmp;
            } else {
                int cmp = Long.signum(1L - weight);
                if (cmp != 0)
                    return cmp;
            }
            return super.compareTo(o);
        }

        @Override
        public int compareTo (IntArrayList o){
            if (o instanceof IdentifiableWeightedSequence) {
                int cmp = Long.signum(((IdentifiableWeightedSequence) o).weight - weight); // descending
                if (cmp != 0)
                    return cmp;
            } else {
                int cmp = Long.signum(1L - weight);
                if (cmp != 0)
                    return cmp;
            }
            return super.compareTo(o);
        }

        @Override
        public boolean equals (Object o){
            if (this == o) return true;
            if (o == null || !(o instanceof IdentifiableWeightedSequence)) return false;
            IdentifiableWeightedSequence that = (IdentifiableWeightedSequence) o;
            if (weight != that.weight) return false;
            return super.equals(that);
        }

        @Override
        public int hashCode () {
            return 31 * super.hashCode() + (int) (weight ^ (weight >>> 32));
        }

        @Override
        public String toString () {
            if (weight == 1) {
                return super.toString();
            } else {
                return super.toString() + "@" + weight;
            }
        }

        @Override
        public void write (DataOutput out) throws IOException {
            WritableUtils.writeVLong(out, weight);
            WritableUtils.writeVLong(out, id);
            super.write(out);
        }

        @Override
        public void readFields (DataInput in) throws IOException {
            weight = WritableUtils.readVLong(in);
            id = WritableUtils.readVLong(in);
            super.readFields(in);
        }

        @Override
        public void writeExternal (ObjectOutput out) throws IOException {
            write(out);
        }

        @Override
        public void readExternal (ObjectInput in) throws IOException, ClassNotFoundException {
            readFields(in);
        }

        public static final class KryoSerializer extends Writable2Serializer<IdentifiableWeightedSequence> {
            @Override
            public IdentifiableWeightedSequence newInstance(Kryo kryo, Input input, Class<IdentifiableWeightedSequence> type) {
                return new IdentifiableWeightedSequence(1L, (int[]) null, 1L);
            }
        }

    }
