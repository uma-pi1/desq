package de.uni_mannheim.desq.dictionary;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.commons.lang.NotImplementedException;

import java.util.BitSet;
import java.util.Set;

/** A subset of a given dictionary that contains only the specified items (including the *direct* links between these items).
 *
 * TODO: also add indirect links? (takes some thought to figure out which links to acutally add and how to do this
 *       reasonably efficiently. One option: compute transitive closure, drop items to be removed, then compute
 *       transitive reduction.)
 *
 * Use with care. This is mainly thought for internal use. Some methods are not implemented or may have slightly
 * different semantics.
 */
public class RestrictedDictionary extends Dictionary {
    private BitSet availableFids = new BitSet();

    public RestrictedDictionary(Dictionary dict, IntSet fidsToRetain) {
        super(dict, true);

        // copy parents and children
        IntIterator it = fidsToRetain.iterator();
        while (it.hasNext()) {
            int fid = it.nextInt();
            availableFids.set(fid);

            // copy parents
            while (parents.size() <= fid) parents.add(null);
            IntArrayList parentFids = dict.parentsOf(fid);
            parents.set(fid, restrict(parentFids, fidsToRetain));

            // copy children
            while (children.size() <= fid) children.add(null);
            IntArrayList childrenFids = dict.childrenOf(fid);
            children.set(fid, restrict(childrenFids, fidsToRetain));
        }

        size = availableFids.cardinality();
        isForest = null;
        hasConsistentFids = null;
        largestRootFid = null;
    }

    private static IntArrayList restrict(IntArrayList fids, IntSet fidsToRetain) {
        // count how many fids are contained in l
        int count = 0;
        IntIterator it = fids.iterator();
        while (it.hasNext()) {
            int fid = it.next();
            if (fidsToRetain.contains(fid)) {
                count++;
            }
        }

        // if everything is retained, just return the list
        if (count == fids.size())
            return fids;

        // else we need to copy
        IntArrayList retainedFids = new IntArrayList(count);
        it = fids.iterator();
        while (it.hasNext()) {
            int fid = it.next();
            if (fidsToRetain.contains(fid)) {
                retainedFids.add(fid);
            }
        }
        return retainedFids;
    }

    /** Always true. */
    public boolean isReadOnly() {
        return true;
    }

    public boolean containsFid(int fid) {
        return availableFids.get(fid);
    }

    public boolean containsGid(int gid) {
        int fid = super.fidOf(gid);
        return fid >= 0 ? containsFid(fid) : false;
    }

    public boolean containsSid(String sid) {
        int fid = super.fidOf(sid);
        return fid >= 0 ? containsFid(fid) : false;
    }

    /** Returns the smallest fid or -1 if the dictionary is empty. */
    public int firstFid() {
        return availableFids.nextSetBit(1);
    }

    /** Returns the next-largest fid to the provided one or -1 if no more fids exist. */
    public int nextFid(int fid) {
        return availableFids.nextSetBit(fid+1);
    }

    /** Returns the next-smallest fid to the provided one or -1 if no more fids exist. */
    public int prevFid(int fid) {
        return availableFids.previousSetBit(fid-1);
    }

    /** Returns the largest fid or -1 if the dictionary is empty. */
    public int lastFid() {
        return availableFids.length()-1;
    }

    public IntCollection fids() {
        throw new NotImplementedException();
    }

    public IntCollection gids() {
        throw new NotImplementedException();
    }

    public Set<String> sids() {
        throw new NotImplementedException();
    }
}
