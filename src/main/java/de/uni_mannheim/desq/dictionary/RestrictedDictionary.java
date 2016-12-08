package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.IntListOptimizer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang.NotImplementedException;

import java.util.BitSet;

/** A subset of a given dictionary that contains only the specified items (including the *direct* links between
 * these items).
 *
 * TODO: also add indirect links? (takes some thought to figure out which links to acutally add and how to do this
 *       reasonably efficiently. One option: compute transitive closure, drop items to be removed, then compute
 *       transitive reduction.)
 *
 * Use with care. This is mainly thought for internal use. Some methods are not implemented or may have slightly
 * different semantics.
 */
public class RestrictedDictionary extends BasicDictionary {
    private BitSet availableFids;

    /** fidsToRetain is stored internally so don't modify */
    public RestrictedDictionary(BasicDictionary dict, BitSet fidsToRetain) {
        super(dict, false);
        availableFids = fidsToRetain;

        // copy parents and children
        IntArrayList tempList = new IntArrayList();
        IntListOptimizer optimizer = new IntListOptimizer(false);
        for (int fid = fidsToRetain.nextSetBit(0); fid >= 0; fid = fidsToRetain.nextSetBit(fid+1)) {
            // copy parents
            while (parents.size() <= fid) parents.add(null);
            IntList parentFids = dict.parentsOf(fid);
            parents.set(fid, restrict(optimizer, parentFids, fidsToRetain, tempList));

            // copy children
            while (children.size() <= fid) children.add(null);
            IntList childrenFids = dict.childrenOf(fid);
            children.set(fid, restrict(optimizer, childrenFids, fidsToRetain, tempList));
        }

        size = availableFids.cardinality();
        isForest = null;
        hasConsistentFids = null;
        largestRootFid = null;
    }

    private static IntList restrict(IntListOptimizer optimizer, IntList fids, BitSet fidsToRetain, IntArrayList tempList) {
        tempList.clear();
        for (int i=0; i<fids.size(); i++) {
            int fid = fids.getInt(i);
            if (fidsToRetain.get(fid)) {
                tempList.add(fid);
            }
        }
        return tempList.size() != fids.size() ? optimizer.optimize(tempList) : fids;
    }

    @Override
    public boolean containsFid(int fid) {
        return availableFids.get(fid);
    }

    @Override
    public boolean containsGid(int gid) {
        int fid = super.fidOf(gid);
        return fid >= 0 ? containsFid(fid) : false;
    }

    /** Returns the smallest fid or -1 if the dictionary is empty. */
    @Override
    public int firstFid() {
        return availableFids.nextSetBit(1);
    }

    /** Returns the next-largest fid to the provided one or -1 if no more fids exist. */
    @Override
    public int nextFid(int fid) {
        return availableFids.nextSetBit(fid+1);
    }

    /** Returns the next-smallest fid to the provided one or -1 if no more fids exist. */
    @Override
    public int prevFid(int fid) {
        return availableFids.previousSetBit(fid-1);
    }

    /** Returns the largest fid or -1 if the dictionary is empty. */
    @Override
    public int lastFid() {
        return availableFids.length()-1;
    }

    @Override
    public IntCollection fids() {
        throw new NotImplementedException();
    }

    @Override
    public IntCollection gids() {
        throw new NotImplementedException();
    }

    public RestrictedDictionary deepCopy() {
        throw new NotImplementedException();
    }
}
