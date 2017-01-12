package de.uni_mannheim.desq.dictionary;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;

/** A {@link BasicDictionary} which serializes only fids. An MiningDictionary instance is fixed to one sigma value
 *
 * Mainly used internally during mining. To obtain a basic dictionary, use
 * {@link Dictionary#shallowCopyAsBasicDictionary()} or {@link Dictionary#deepCopyAsBasicDictionary()}.
 */
public class MiningDictionary extends BasicDictionary {
    /** Stores the last frequent fid of this dictionary for one specific support (sigma) value **/
    private int lastFrequentFid;

    public MiningDictionary() {
        super();
    }

    /** Shallow clone. Creates a new dictionary backed by given dictionary.
     *
     * @param other
     * @param withLinks if set, parents and childrens are retained
     */
    protected MiningDictionary(BasicDictionary other, boolean withLinks, long sigma) {
        super(other, withLinks);
        this.lastFrequentFid = lastFidAbove(sigma);
        System.out.println("last frequent fid: " + lastFrequentFid);
    }

    /** Ensures sufficent storage storage for items */
    @Override
    protected void ensureCapacity(int capacity) {
//        gids.ensureCapacity(capacity+1);
//        CollectionUtils.ensureCapacity(dfreqs, capacity+1);
//        CollectionUtils.ensureCapacity(cfreqs, capacity+1);
        parents.ensureCapacity(capacity+1);
        children.ensureCapacity(capacity+1);
    }

    /** Returns the stored last frequent fid. This fid is specific to a specific support value. This support value
     * needs to be passed to the MiningDictionary constructor when creating the object the first time.
     */
    public int lastFrequentFid() {
        return lastFrequentFid;
    }

    @Override
    public boolean containsFid(int fid) {
        // as we don't store gids after deserializing, we need to do this with parents
        return fid < parents.size(); //gids.size() && gids.getInt(fid) >= 0;
    }

    /** Returns the largest fid or -1 if the dictionary is empty. */
    public int lastFid() {
        int fid = parents.size()-1;
        return fid;
//        while (fid>=0) {
//            if (gids.getInt(fid) >= 0) return fid;
//            fid--;
//        }
//        return -1;
    }

    // -- serialization -----------------------------------------------------------------------------------------------

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // general info
        WritableUtils.writeVInt(out, size());
        out.writeBoolean(isForest());
        out.writeBoolean(hasConsistentFids());
        WritableUtils.writeVInt(out, largestRootFid());
        WritableUtils.writeVInt(out, lastFrequentFid);

        // each item
        IntList fids = topologicalSort();
        for (int i=0; i<fids.size(); i++) {
            int fid = fids.getInt(i);
            WritableUtils.writeVInt(out, fid);
//            WritableUtils.writeVInt(out, gids.getInt(fid));
//            WritableUtils.writeVLong(out, dfreqs.getLong(fid));
//            WritableUtils.writeVLong(out, cfreqs.getLong(fid));

            IntList parents = this.parents.get(fid);
            WritableUtils.writeVInt(out, parents.size());
            IntList children = this.children.get(fid);
            WritableUtils.writeVInt(out, children.size());

            for (int j=0; j<parents.size(); j++) {
                WritableUtils.writeVInt(out, parents.getInt(j));
            }
        }
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        clear();

        // general info
        int size = WritableUtils.readVInt(in);
        ensureCapacity(size);
        boolean isForest = in.readBoolean();
        boolean hasConsistentFids = in.readBoolean();
        largestRootFid = WritableUtils.readVInt(in);
        lastFrequentFid = WritableUtils.readVInt(in);

        // each item
        for (int i=0; i<size; i++) {
            int fid = WritableUtils.readVInt(in);
//            int gid = WritableUtils.readVInt(in);
//            long dfreq = WritableUtils.readVLong(in);
//            long cfreq = WritableUtils.readVLong(in);

            int noParents = WritableUtils.readVInt(in);
            IntArrayList parents = new IntArrayList(noParents);
            int noChildren = WritableUtils.readVInt(in);
            IntArrayList children = new IntArrayList(noChildren);

//            addItem(fid, gid, dfreq, cfreq, parents, children);
            addItem(fid, parents, children);

            for (int j=0; j<noParents; j++) {
                addParent(fid, WritableUtils.readVInt(in));
            }
        }

        assert this.size == size;
        this.isForest = isForest;
        this.hasConsistentFids = hasConsistentFids;
    }

    /** Adds a new item to this dictionary. */
    public void addItem(int fid, IntArrayList parents, IntArrayList children) {
        if (fid <= 0)
            throw new IllegalArgumentException("fid '" + fid + "' is not positive");
//        if (containsFid(fid))
//            throw new IllegalArgumentException("Item fid '" + fid + "' exists already");
//        if (gid < 0)
//            throw new IllegalArgumentException("gid '" + gid + "' is negative");
//        if (containsGid(gid))
//            throw new IllegalArgumentException("Item gid '" + gid + "' exists already");

        // create enough space by inserting dummy values
        while (this.parents.size() <= fid) {
//            gids.add(-1);
//            dfreqs.add(-1);
//            cfreqs.add(-1);
            this.parents.add(null);
            this.children.add(null);
        }

        // now put the item
//        gids.set(fid, gid);
//        dfreqs.set(fid, dfreq);
//        cfreqs.set(fid, cfreq);
        this.parents.set(fid, parents);
        this.children.set(fid, children);
//        this.children.set(fid, new IntArrayList());
//        gidIndex.put(gid, fid);

        // update cached information
        size += 1;
        isForest = null;
        hasConsistentFids = null;
        largestRootFid = null;
    }

    public void addParent(int childFid, int parentFid) {
//        if (!containsFid(childFid) || !containsFid(parentFid))
//            throw new IllegalArgumentException();
        parentsOf(childFid).add(parentFid);
        childrenOf(parentFid).add(childFid);
    }

    public void clear() {
        size = 0;
//        gids.clear();
//        dfreqs.clear();
//        cfreqs.clear();
        parents.clear();
        children.clear();
//        gidIndex.clear();
        isForest = null;
        hasConsistentFids = null;
        largestRootFid = null;
    }

    public static final class KryoSerializer extends Writable2Serializer<MiningDictionary> {
        @Override
        public MiningDictionary newInstance(Kryo kryo, Input input, Class<MiningDictionary> type) {
            return new MiningDictionary();
        }
    }
}
