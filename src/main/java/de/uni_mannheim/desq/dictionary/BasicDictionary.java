package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.util.CollectionUtils;
import de.uni_mannheim.desq.util.IntListOptimizer;
import de.uni_mannheim.desq.util.LongIntArrayList;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.ArrayList;
import java.util.Collections;

/** A {@link Dictionary} without string identifiers and support for modification.
 *
 * Mainly used internally during mining. To obtain a basic dictionary, use
 * {@link Dictionary#shallowCopyAsBasicDictionary()} or {@link Dictionary#deepCopyAsBasicDictionary()}.
 */
public class BasicDictionary {
    // -- information about items -------------------------------------------------------------------------------------

    /** The number of items in this dictionary. */
    protected int size = 0;

    /** Stable global identifiers of this dictionary's items. Indexed by fid; -1 if fid not present.  */
    protected final IntArrayList gids;

    /** The document frequencies of this dictionary's items. Indexed by fid; -1 if fid not present. */
    protected LongList dfreqs;

    /** The collection frequencies of this dictionary's items. Indexed by fid; -1 if fid not present. */
    protected LongList cfreqs;

    /** The fids of the parents of this dictionary's items. Indexed by fid; null if fid not present. */
    protected final ArrayList<IntList> parents;

    /** The fids of the children of this dictionary's items. Indexed by fid; null if fid not present. */
    protected final ArrayList<IntList> children;

    // -- indexes -----------------------------------------------------------------------------------------------------

    /** Maps gids to fids. */
    protected final Int2IntOpenHashMap gidIndex;

    // -- information about this dictionary ---------------------------------------------------------------------------

    /** Whether this dictionary is a forest or <code>null</code> if unknown. See {@link #isForest()}. */
    protected Boolean isForest = null;

    /** Whether the fids in this dictionary are consistent or <code>null</code> if unknown. See {@link #hasConsistentFids()}. */
    protected Boolean hasConsistentFids = null;

    /** The largest FID of a root item or <code>null</code> if unknown. See {@link #largestRootFid()}. */
    protected Integer largestRootFid = null;

    /** Constructor that initializes all data structures. */
    protected BasicDictionary() {
        gids = new IntArrayList();
        dfreqs = new LongArrayList();
        cfreqs = new LongArrayList();
        parents = new ArrayList<>();
        children = new ArrayList<>();
        gidIndex = new Int2IntOpenHashMap();
        gidIndex.defaultReturnValue(-1);
    }

    /** Deep clone. */
    protected BasicDictionary(BasicDictionary other, boolean freeze, boolean dummy) {
        size = other.size;
        gids = other.gids.clone();
        if (freeze && LongIntArrayList.fits(other.dfreqs)) {
            dfreqs = new LongIntArrayList(other.dfreqs);
        } else {
            dfreqs = new LongArrayList(other.dfreqs);
        }
        if (freeze && LongIntArrayList.fits(other.cfreqs)) {
            cfreqs = new LongIntArrayList(other.cfreqs);
        } else {
            cfreqs = new LongArrayList(other.cfreqs);
        }
        gidIndex = other.gidIndex.clone();
        parents = new ArrayList<>(gids.size());
        children = new ArrayList<>(gids.size());
        isForest = other.isForest;
        hasConsistentFids = other.hasConsistentFids;
        largestRootFid = other.largestRootFid;
        IntListOptimizer optimizer = new IntListOptimizer(false);
        for (int i=0; i<gids.size(); i++) {
            int gid = gids.getInt(i);
            if (gid >= 0) {
                if (freeze) {
                    parents.add(optimizer.optimize(other.parents.get(i)));
                    children.add(optimizer.optimize(other.children.get(i)));
                } else {
                    parents.add(new IntArrayList(other.parents.get(i)));
                    children.add(new IntArrayList(other.children.get(i)));
                }
            } else {
                parents.add(null);
                children.add(null);
            }
        }
    };

    /** Shallow clone. Creates a new dictionary backed by given dictionary.
     *
     * @param other
     * @param withLinks if set, parents and childrens are retained
     */
    protected BasicDictionary(BasicDictionary other, boolean withLinks) {
        gids = other.gids;
        dfreqs = other.dfreqs;
        cfreqs = other.cfreqs;
        if (withLinks) {
            parents = other.parents;
            children = other.children;
        } else {
            parents = new ArrayList<>();
            children = new ArrayList<>();
        }
        gidIndex = other.gidIndex;
    }

    // -- modification ------------------------------------------------------------------------------------------------

    /** Ensures sufficent storage storage for items */
    protected void ensureCapacity(int capacity) {
        gids.ensureCapacity(capacity+1);
        CollectionUtils.ensureCapacity(dfreqs, capacity+1);
        CollectionUtils.ensureCapacity(cfreqs, capacity+1);
        parents.ensureCapacity(capacity+1);
        children.ensureCapacity(capacity+1);
    }

    /** Reduces the memory footprint of this dictionary as much as possible. */
    public void trim() {
        int newSize = lastFid()+1;
        CollectionUtils.trim(gids, newSize);
        CollectionUtils.trim(dfreqs, newSize);
        CollectionUtils.trim(cfreqs, newSize);
        CollectionUtils.trim(parents, newSize);
        CollectionUtils.trim(children, newSize);
        gidIndex.trim();

        for (int fid=firstFid(); fid>=0; fid=nextFid(fid)) {
            CollectionUtils.trim( parentsOf(fid) );
            CollectionUtils.trim( childrenOf(fid) );
        }
    }

    /** Freezes this dictionary. When calling this method, the dictionary is reorganized to save memory. */
    protected void freeze() {
        IntListOptimizer optimizer = new IntListOptimizer(true);

        // optimize parents
        for (int i=0; i<parents.size(); i++) {
            IntList l = parents.get(i);
            if (l==null) continue;
            parents.set(i, optimizer.optimize(l));
        }

        // optimize children
        for (int i=0; i<children.size(); i++) {
            IntList l = children.get(i);
            if (l==null) continue;
            children.set(i, optimizer.optimize(l));
        }

        // optimize frequencies
        if (!(dfreqs instanceof LongIntArrayList) && LongIntArrayList.fits(dfreqs)){
            dfreqs = new LongIntArrayList(dfreqs);
        }
        if (!(cfreqs instanceof LongIntArrayList) && LongIntArrayList.fits(cfreqs)){
            cfreqs = new LongIntArrayList(cfreqs);
        }

        // now trim everything
        trim();
    }

    /** Returns a memory-optimized deep copy of this dictionary. */
    public BasicDictionary deepCopy() {
        return new BasicDictionary(this, true, false);
    }

    // -- querying ----------------------------------------------------------------------------------------------------

    /** The number of items in this dictionary. */
    public int size() {
        return size;
    }

    public boolean containsFid(int fid) {
        return fid < gids.size() && gids.getInt(fid) >= 0;
    }

    public boolean containsGid(int gid) {
        return gidIndex.containsKey(gid);
    }

    /** Returns the smallest fid or -1 if the dictionary is empty. */
    public int firstFid() {
        return nextFid(0);
    }

    /** Returns the next-largest fid to the provided one or -1 if no more fids exist. */
    public int nextFid(int fid) {
        do {
            fid++;
            if (fid >= gids.size()) return -1;
            if (gids.getInt(fid) >= 0) return fid;
        } while (true);
    }

    /** Returns the next-smallest fid to the provided one or -1 if no more fids exist. */
    public int prevFid(int fid) {
        do {
            fid--;
            if (fid < 0) return -1;
            if (gids.getInt(fid) >= 0) return fid;
        } while (true);
    }

    /** Returns the largest fid or -1 if the dictionary is empty. */
    public int lastFid() {
        int fid = gids.size()-1;
        while (fid>=0) {
            if (gids.getInt(fid) >= 0) return fid;
            fid--;
        }
        return -1;
    }

    public IntIterator fidIterator() {
        return new AbstractIntIterator() {
            int nextFid = firstFid();

            @Override
            public boolean hasNext() {
                return nextFid >= 0;
            }

            @Override
            public int nextInt() {
                int result = nextFid;
                nextFid = nextFid(result);
                return result;
            }
        };
    }

    /** Returns the fid for the specified gid or -1 if not present */
    public int fidOf(int gid) {
        return gidIndex.get(gid);
    }


    /** Returns the gid for the specified fid or -1 if not present */
    public int gidOf(int fid) {
        return fid<gids.size() ? gids.getInt(fid) : -1;
    }

    /** Returns all fids. */
    public IntCollection fids() {
        return gidIndex.values();
    }

    /** Returns all gids. */
    public IntCollection gids() {
        return gidIndex.keySet();
    }

    /** Returns the document frequency of the specified fid or -1 if not present */
    public long dfreqOf(int fid) {
        return dfreqs.getLong(fid);
    }

    /** Returns the collection frequency of the specified fid or -1 if not present */
    public long cfreqOf(int fid) {
        return cfreqs.getLong(fid);
    }

    /** Returns the fids of the children of the specified fid or -1 if not present */
    public IntList childrenOf(int fid) {
        return children.get(fid);
    }

    public boolean isLeaf(int fid) {
        return children.get(fid).isEmpty();
    }

    /** Returns the fids of the parents of the specified fid or -1 if not present */
    public IntList parentsOf(int fid) {
        return parents.get(fid);
    }

    /** Determines whether the items in this dictionary form a forest.  */
    public boolean isForest() {
        if (isForest != null) {
            return isForest;
        }
        isForest = true;
        for (int fid=firstFid(); fid >= 0; fid=nextFid(fid)) {
            if (parentsOf(fid).size() > 1) {
                isForest = false;
                break;
            }
        }
        return isForest;
    }

    /** Checks whether fids are assigned such that: (1) items with higher document frequency have
     * lower fid and * (2) parents have lower fids than their children. Assumes that supports are consistent
     * (i.e., parents never have lower document frequency than one of their children).
     *
     * If this method returns false, use {@link Dictionary#recomputeFids()} to change the fids accordingly.
     */
    public boolean hasConsistentFids() {
        if (hasConsistentFids != null) {
            return hasConsistentFids;
        }

        hasConsistentFids = true;
        long lastDfreq = Long.MAX_VALUE;

        loop:
        for (int fid=firstFid(); fid >= 0; fid=nextFid(fid)) {
            // check frequency
            long dfreq = dfreqOf(fid);
            if (dfreq > lastDfreq) {
                hasConsistentFids = false;
                break loop;
            }
            lastDfreq = dfreq;

            // check parents
            IntList parents = parentsOf(fid);
            for (int i=0; i<parents.size(); i++) {
                if (parents.getInt(i) >= fid) {
                    hasConsistentFids = false;
                    break loop;
                }
            }
        }
        return hasConsistentFids;
    }


    /** Determines the fid of the root item with the largest fid. */
    public int largestRootFid() {
        if (largestRootFid != null) {
            return largestRootFid;
        }
        for (int fid=lastFid(); fid >= 0; fid=prevFid(fid)) {
            if (parentsOf(fid).size() == 0) {
                largestRootFid = fid;
                return largestRootFid;
            }
        }

        // every dictionary has at least one root, so we shouldn't reach this place
        throw new IllegalStateException();
    }

    /** Gets the largest fid of in item with document frequency at least as large as specified. Returns -1 if
     * there is no such item. */
    public int lastFidAbove(long dfreq) {
        for (int fid = lastFid(); fid >= 0; fid=prevFid(fid)) {
            if (dfreqs.getLong(fid)>=dfreq) return fid;
        }
        return -1;
    }

    // -- gid/fid conversion -----------------------------------------------------------------------------------------

    /** in-place */
    public void gidsToFids(IntList items) {
        for (int i=0; i<items.size(); i++) {
            int gid = items.getInt(i);
            int fid = fidOf(gid);
            items.set(i, fid);
        }
    }

    public void gidsToFids(IntList gids, IntList fids) {
        fids.size(gids.size());
        for (int i=0; i<gids.size(); i++) {
            int gid = gids.getInt(i);
            int fid = fidOf(gid);
            fids.set(i, fid);
        }
    }

    /** in-place */
    public void fidsToGids(IntList items) {
        for (int i=0; i<items.size(); i++) {
            int fid = items.getInt(i);
            int gid = gidOf(fid);
            items.set(i, gid);
        }
    }

    public void fidsToGids(IntList fids, IntList gids) {
        gids.size(fids.size());
        for (int i=0; i<fids.size(); i++) {
            int fid = fids.getInt(i);
            int gid = gidOf(fid);
            gids.set(i, gid);
        }
    }

    // -- computing descendants and ascendants ------------------------------------------------------------------------

    /** Returns the fids of all descendants of the given item (including the given item) */
    public IntSet descendantsFids(int fid) {
        return descendantsFids(IntSets.singleton(fid));
    }

    /** Returns the fids of all descendants of the given items (including the given items) */
    public IntSet descendantsFids(IntCollection fids) {
        IntSet descendants = new IntOpenHashSet();
        IntIterator it = fids.iterator();
        while (it.hasNext()) {
            int fid = it.nextInt();
            if (descendants.add(fid)) {
                addDescendantFids(fid, descendants);
            }
        }
        return descendants;
    }

    /** Adds all descendants of the specified item to fids, excluding the given item and all
     * descendants of items already present in itemFids. */
    public void addDescendantFids(int fid, IntSet fids) {
        IntList children = childrenOf(fid);
        for (int i=0; i<children.size(); i++) {
            int childFid = children.getInt(i);
            if (fids.add(childFid)) {
                addDescendantFids(childFid, fids);
            }
        }
    }


    /** Returns the fids of all ascendants of the given item (including the given item) */
    public IntSet ascendantsFids(int fid) {
        return ascendantsFids(IntSets.singleton(fid));
    }

    /** Checks whether the given item or one of its ascendants has a fid below the given one. Quick way to check whether
     * an item has a frequent fid when {@link #hasConsistentFids} is true. */
    public boolean hasAscendantWithFidBelow(int fid, int maxFid) {
        // TODO: may be slow when there are lots of ancendants that can be reached via multiple paths and are all
        // infrequent
        if (fid <= maxFid) return true;
        IntList parents = parentsOf(fid);
        for (int i=0; i<parents.size(); i++) {
            int parentFid = parents.getInt(i);
            if (hasAscendantWithFidBelow(parentFid, maxFid))
                return true;
        }
        return false;
    }

    /** Returns the fids of all ascendants of the given items (including the given items) */
    public IntSet ascendantsFids(IntCollection fids) {
        IntSet ascendants = new IntOpenHashSet();
        IntIterator it = fids.iterator();
        while (it.hasNext()) {
            int fid = it.nextInt();
            if (ascendants.add(fid)) {
                addAscendantFids(fid, ascendants);
            }
        }
        return ascendants;
    }

    /** Adds all ascendants of the specified item to fids, excluding the given item and all
     * ascendants of items already present in itemFids. This method is performance-critical for many mining methods.
     *
     * If the dictionary does not form a forest (see {@link #isForest()}) or if <code>fids</code> is not initially
     * empty, <code>fids</code> *must* be an {@link IntSet}, ideally one that allows for fast look-ups
     * (such as {@link IntOpenHashSet} or{@link IntAVLTreeSet}). Otherwise, for best performance, pass an
     * {@link IntArrayList}.
     */
    public final void addAscendantFids(int fid, IntCollection fids) {
        assert fids instanceof IntSet || (isForest() && fids.isEmpty());
        if (fids.isEmpty())
            addAscendantFids(fid, fids, Integer.MAX_VALUE);
        else
            addAscendantFids(fid, fids, Integer.MIN_VALUE);
    }

    // minFidInItemFids is a lowerbound of the smallest fid currently in the set. This method makes use of the
    // fact that parents have smaller fids than their children.
    private int addAscendantFids(int fid, IntCollection fids, int minFidInFids) {
        IntList parents = parentsOf(fid);
        for (int i=0; i<parents.size(); i++) {
            int parentFid = parents.getInt(i);
            if (parentFid < minFidInFids) { // must be new
                fids.add(parentFid);
                minFidInFids = parentFid;
                minFidInFids = addAscendantFids(parentFid, fids, minFidInFids);
            } else if (fids.add(parentFid)) { // else look-up and ignore if present
                assert fids instanceof IntSet;
                minFidInFids = addAscendantFids(parentFid, fids, minFidInFids);
            }
        }
        return minFidInFids;
    }

    /** Adds all ascendants of the specified item to gids, excluding the given item and all
     * ascendants of items already present in gids. */
    public void addAscendantGids(int gid, IntSet gids) {
        addAscendantGidsFromFid(fidOf(gid), gids);
    }

    private void addAscendantGidsFromFid(int fid, IntSet gids) {
        IntList parents = parentsOf(fid);
        for (int i=0; i<parents.size(); i++) {
            int parentFid = parents.getInt(i);
            if (gids.add(gidOf(parentFid))) {
                addAscendantGidsFromFid(parentFid, gids);
            }
        }
    }

    // -- computing supports ------------------------------------------------------------------------------------------

    /** Computes the item collection frequencies of the items in a sequence (including ancestors).
     *
     * @param sequence the input sequence
     * @param itemCfreqs stores the result (map from item identifier to frequency)
     * @param ancItems a temporary set used by this method (for reuse; won't be read but is modified)
     * @param usesFids whether the input sequence consists of gid or fid item identifiers
     * @param weight the weight (frequency) of the sequence
     */
    public void computeItemCfreqs(IntCollection sequence, Int2LongMap itemCfreqs, IntSet ancItems, boolean usesFids, long weight) {
        assert itemCfreqs.defaultReturnValue() <= 0;
        itemCfreqs.clear();
        IntIterator it = sequence.iterator();
        while(it.hasNext()) {
            int ii = it.nextInt();
            ancItems.clear();
            ancItems.add(ii);
            if (usesFids) {
                addAscendantFids(ii, ancItems);
            } else {
                addAscendantGids(ii, ancItems);
            }
            for (int item : ancItems) {
                long oldValue = itemCfreqs.put(item, weight);
                if (oldValue > 0) {
                    itemCfreqs.put(item, oldValue + weight);
                }
            }
        }
    }

    // -- computing fids --------------------------------------------------------------------------

    /** Performs a topological sort of the items in this dictionary, respecting document
     * frequencies. Throws an IllegalArgumentException if there is a cycle.
     *
     * @returns item fids in topological order
     * @author Keith Schwarz (htiek@cs.stanford.edu)
     */
    public IntList topologicalSort() {
        /* Maintain two structures - a set of visited nodes (so that once we've
         * added a node to the list, we don't label it again), and a list of
         * nodes that actually holds the topological ordering.
         */
        IntList resultFids = new IntArrayList();
        IntSet visitedFids = new IntOpenHashSet();

        /* We'll also maintain a third set consisting of all nodes that have
         * been fully expanded.  If the graph contains a cycle, then we can
         * detect this by noting that a node has been explored but not fully
         * expanded.
         */
        IntSet expandedFids = new IntOpenHashSet();

        // Sort the all items by decreasing support. This way,
        // items with a higher frequency will always appear before items
        // with lower frequency (under the assumption that document frequencies
        // are valid).
        IntList fids = new IntArrayList(size());
        for (int fid = firstFid(); fid >= 0; fid = nextFid(fid)) {
            fids.add(fid);
        }
        Collections.sort(fids, (fid1, fid2) -> Long.compare(dfreqOf(fid2), dfreqOf(fid1)));

        /* Fire off a DFS from each node in the graph. */
        IntListIterator it = fids.iterator();
        while (it.hasNext()) {
            explore(it.nextInt(), resultFids, visitedFids, expandedFids);
        }

        /* Hand back the resulting ordering. */
        return resultFids;
    }

    /**
     * Recursively performs a DFS from the specified node, marking all nodes
     * encountered by the search.
     *
     * @param fid The node to begin the search from.
     * @param orderingFids A list holding the topological sort of the graph.
     * @param visitedFids A set of nodes that have already been visited.
     * @param expandedFids A set of nodes that have been fully expanded.
     *
     * @author Keith Schwarz (htiek@cs.stanford.edu)
     */
    private void explore(int fid, IntList orderingFids, IntSet visitedFids,
                         IntSet expandedFids) {
        /* Check whether we've been here before.  If so, we should stop the
         * search.
         */
        if (visitedFids.contains(fid)) {
            /* There are two cases to consider.  First, if this node has
             * already been expanded, then it's already been assigned a
             * position in the final topological sort and we don't need to
             * explore it again.  However, if it hasn't been expanded, it means
             * that we've just found a node that is currently being explored,
             * and therefore is part of a cycle.  In that case, we should
             * report an error.
             */
            if (expandedFids.contains(fid)) return;
            throw new IllegalArgumentException("Graph contains a cycle.");
        }

        /* Mark that we've been here */
        visitedFids.add(fid);

        /* Recursively explore all of the node's predecessors. */
        IntList parents = parentsOf(fid);
        for (int i=0; i<parents.size(); i++)
            explore(parents.get(i), orderingFids, visitedFids, expandedFids);

        /* Having explored all of the node's predecessors, we can now add this
         * node to the sorted ordering.
         */
        orderingFids.add(fid);

        /* Similarly, mark that this node is done being expanded. */
        expandedFids.add(fid);
    }

}
