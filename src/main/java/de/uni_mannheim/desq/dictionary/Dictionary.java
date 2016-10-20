package de.uni_mannheim.desq.dictionary;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.avro.AvroItem;
import de.uni_mannheim.desq.avro.AvroItemProperties;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.DataInput2InputStreamWrapper;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.IntSetUtils;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** A set of items arranged in a hierarchy. Each item is associated with a gid (stable numeric item identifier),
 * a sid (stable string item identifier) and a fid (variable frequency identifier / position). */
public final class Dictionary implements Externalizable, Writable {

	// -- information about items -------------------------------------------------------------------------------------

	/** Stable global identifiers of this dictionary's items. The position of an item in this list is its fid. */
	private final IntArrayList gids = new IntArrayList();

	/** Stable unique item names of this dictionary's items. Indexed as {@link #gids}. */
	private final ArrayList<String> sids = new ArrayList<>();

	/** The supports (document frequencies) of this dictionary's items. Indexed as {@link #gids}. */
	private final LongArrayList supports = new LongArrayList();

	/** The collection supports (collection frequencies) of this dictionary's items. Indexed as {@link #gids}. */
	private final LongArrayList cSupports = new LongArrayList();

	/** The positions of the parents of this dictionary's items. Indexed as {@link #gids}. */
	private final ArrayList<IntArrayList> parents = new ArrayList<>();

	/** The positions of the children of this dictionary's items. Indexed as {@link #gids}. */
	private final ArrayList<IntArrayList> children = new ArrayList<>();

	/** Properties associated with this dictionary's items. Indexed as {@link #gids}. */
	private final ArrayList<DesqProperties> properties = new ArrayList<>();


	// -- indexes -----------------------------------------------------------------------------------------------------

	/** Maps a gid to a fid */
	private final Int2IntOpenHashMap gidIndex = new Int2IntOpenHashMap();

	/** Maps a sid to a fid */
	private final Object2IntOpenHashMap<String> sidIndex = new Object2IntOpenHashMap<>();


	// -- information about this dictionary ---------------------------------------------------------------------------

	/** Whether this dictionary is a forest or <code>null</code> if unknown. See {@link #isForest()}. */
    private Boolean isForest = null;

	/** Whether the fids in this dictionary are valid  <code>null</code> if unknown. See {@link #hasValidFids()}. */
	private Boolean hasValidFids = null;


	// -- construction and modification -------------------------------------------------------------------------------

	public Dictionary() {
		gidIndex.defaultReturnValue(-1);
		sidIndex.defaultReturnValue(-1);
	}

	/** Adds an item and returns the (preliminary) fid assigned to the added item. */
	private int setItem(int fid, int gid, String sid, long support, long cSupport,
					   IntArrayList parents, IntArrayList children, DesqProperties properties) {
		if (gidIndex.containsKey(gid))
			throw new IllegalArgumentException("Item gid '" + gid + "' exists already");
		if (sidIndex.containsKey(sid))
			throw new IllegalArgumentException("Item sid '" + sid + "' exists already");

		// create enough space by inserting invalid values (need to be set later)
		ensureCapacity(fid+1);
		while (size() <= fid) {
			gids.add(-1);
			sids.add(null);
			supports.add(-1);
			cSupports.add(-1);
			this.parents.add(null);
			this.children.add(null);
			this.properties.add(null);
		}

		// now put the item
		gids.set(fid, gid);
		sids.set(fid, sid);
		supports.set(fid, support);
		cSupports.set(fid, cSupport);
		this.parents.set(fid, parents);
		this.children.set(fid, children);
		this.properties.set(fid, properties);
		gidIndex.put(gid, fid);
		sidIndex.put(sid, fid);

		// invalidate cached information
		isForest = null;
		hasValidFids = null;

		return fid;
	}

	/** Adds an item and returns the fid assigned to the added item. */
	public int addItem(int gid, String sid, long support, long cSupport,
					   IntArrayList parents, IntArrayList children, DesqProperties properties) {
		return setItem(size(), gid, sid, support, cSupport, parents, children, properties);
	}

	public void addParent(int childFid, int parentFid) {
		if (childFid < 0 || childFid > size() || parentFid < 0 || parentFid > size())
			throw new IllegalArgumentException();
		parents(childFid).add(parentFid);
		children(parentFid).add(childFid);
	}

	private void addItem(AvroItem avroItem) {
		// properties
		DesqProperties properties = null;
		if (avroItem.getProperties().size()>0) {
			properties = new DesqProperties(avroItem.getProperties().size());
			for (AvroItemProperties property : avroItem.getProperties()) {
				String key = property.getKey();
				String value = property.getValue() != null ? property.getValue() : null;
				properties.setProperty(key, value);
			}
		}

		// create the item
		int noParents = avroItem.getParentGids().size();
		int noChildren = avroItem.getNoChildren();
		setItem(
				avroItem.getFid(),
				avroItem.getGid(),
				avroItem.getSid(),
				avroItem.getDFreq(),
				avroItem.getCFreq(),
				new IntArrayList(noParents),
				new IntArrayList(noChildren),
				properties
		);

		// and add parents
		int fid = avroItem.getFid();
		for (Integer parentGid : avroItem.getParentGids()) {
			int parentFid = fid(parentGid);
			if (parentFid == -1) {
				throw new RuntimeException("parent item with gid=" + parentGid + " not present");
			}
			addParent(fid, parentFid);
		}
	}

	/** Adds an item and returns the (preliminary) fid assigned to the added item. */
	public int addItem(int gid, String sid) {
		return addItem(gid, sid, 0L, 0L, new IntArrayList(1), new IntArrayList(0), null);
	}

	/** Clears everything */
	public void clear() {
		gids.clear();
		sids.clear();
		supports.clear();
		cSupports.clear();
		parents.clear();
		children.clear();
		properties.clear();
		gidIndex.clear();
		sidIndex.clear();
		isForest = null;
		hasValidFids = null;
	}

	public void clearCounts() {
		for (int i=0; i<size(); i++) {
			supports.set(i, 0L);
			cSupports.set(i, 0L);
			// isForest and hasValidFids can remain unchanged
		}
	}

	public void ensureCapacity(int capacity) {
		gids.ensureCapacity(capacity);
		sids.ensureCapacity(capacity);
		supports.ensureCapacity(capacity);
		cSupports.ensureCapacity(capacity);
		parents.ensureCapacity(capacity);
		children.ensureCapacity(capacity);
		properties.ensureCapacity(capacity);
		// unfortunately, fastutils doesn't allow us to specify the capacity for its hash maps
	}

	public Dictionary deepCopy() {
		// this could be made more efficient with more lines of code
		try {
			byte[] bytes = toBytes();
			return Dictionary.fromBytes(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// -- querying ----------------------------------------------------------------------------------------------------

	public int size() {
		return gids.size();
	}

	/** -1 if not present */
	public int fid(int gid) {
		return gidIndex.get(gid);
	}

	/** -1 if not present */
	public int fid(String sid) {
		return sidIndex.getInt(sid);
	}

	/** error if out of bounds */
	public int gid(int fid) {
		return gids.getInt(fid);
	}

	/** error if out of bounds */
	public int gid(String sid) {
		int fid = fid(sid);
		return fid < 0 ? fid : gid(fid);

	}

	public String sidForFid(int fid) {
		return sids.get(fid);
	}

	public long support(int fid) {
		return supports.getLong(fid);
	}

	public long cSupport(int fid) {
		return cSupports.getLong(fid);
	}

	public IntList children(int fid) {
		return children.get(fid);
	}

	public IntList parents(int fid) {
		return parents.get(fid);
	}

	public DesqProperties properties(int fid) {
		return properties.get(fid);
	}

	/** Determines whether the items in this dictionary form a forest.  */
	public boolean isForest() {
		if (isForest != null) {
			return isForest;
		}
		isForest = true;
		for (int i=0; i<size(); i++) {
			if (parents.get(i).size() > 1) {
				isForest = false;
				break;
			}
		}
		return isForest;
	}

	/** Checks if fids are assigned and indexed correctly. Returns true when (1) items with higher support have
	 * lower fid and * (2) parents have lower fids than their children. Assumes that supports are consistent
	 * (i.e., parents never have lower support than one of their children).
	 *
	 * If this method returns false, use use {@link #recomputeFids()}.
	 */
	public boolean hasValidFids() {
		if (hasValidFids != null) {
			return hasValidFids;
		}

		hasValidFids = true;
		long lastSupport = Long.MAX_VALUE;

		loop:
		for (int fid=0; fid<size(); fid++) {
			// check support
			long support = support(fid);
			if (support > lastSupport) {
				hasValidFids = false;
				break loop;
			}
			lastSupport = support;

			// check parents
			IntList parents = parents(fid);
			for (int i=0; i<parents.size(); i++) {
				if (parents.getInt(i) <= fid) {
					hasValidFids = false;
					break loop;
				}
			}
		}
		return hasValidFids;
	}

	/** Gets the largest fid of in item with document frequency at least as large as specified. Returns -1 if
	 * there is no such item. */
	public int getLargestFidAboveSupport(long support) {
		for (int fid = size()-1; fid >= 0; fid --) {
			if (supports.getLong(fid)>=support) return fid;
		}
		return -1;
	}

	// -- gid/fid conversion -----------------------------------------------------------------------------------------

	/** in-place */
	public void gidsToFids(IntList items) {
		for (int i=0; i<items.size(); i++) {
			int gid = items.getInt(i);
			int fid = fid(gid);
			items.set(i, fid);
		}
	}

	public void gidsToFids(IntList gids, IntList fids) {
		fids.size(gids.size());
		for (int i=0; i<gids.size(); i++) {
			int gid = gids.getInt(i);
			int fid = fid(gid);
			fids.set(i, fid);
		}
	}

	/** in-place */
	public void fidsToGids(IntList items) {
		for (int i=0; i<items.size(); i++) {
			int fid = items.getInt(i);
			int gid = gid(fid);
			items.set(i, gid);
		}
	}

	public void fidsToGids(IntList fids, IntList gids) {
		gids.size(fids.size());
		for (int i=0; i<fids.size(); i++) {
			int fid = fids.getInt(i);
			int gid = gid(fid);
			gids.set(i, gid);
		}
	}

	// -- computing descendants and ascendants ------------------------------------------------------------------------

	/** Returns a suitable IntCollection that can be passed to various methods for ancestor and descendant computation
	 * performed in this class. */
	public IntCollection newFidCollection() {
		if (isForest()) {
			return new IntArrayList();
		} else {
			// TODO: we may want to make a more informed choice here
			return new IntAVLTreeSet();
		}
	}

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
		return IntSetUtils.optimize(descendants);
	}

	/** Adds all descendants of the specified item to fids, excluding the given item and all
	 * descendants of items already present in itemFids. */
	public void addDescendantFids(int fid, IntSet fids) {
		IntList children = children(fid);
		for (int i=0; i<children.size(); i++) {
			int childFid = children.get(i);
			if (fids.add(childFid)) {
				addDescendantFids(childFid, fids);
			}
		}
	}

	/** Returns the fids of all ascendants of the given item (including the given item) */
	public IntSet ascendantsFids(int fid) {
		return ascendantsFids(IntSets.singleton(fid));
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
	 * Best performance is achieved if fids is as obtained from {@link #newFidCollection()}.
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
		IntList parents = parents(fid);
		for (int i=0; i<parents.size(); i++) {
			int parentFid = parents.get(i);
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
		addAscendantGidsFromFid(fid(gid), gids);
	}

	private void addAscendantGidsFromFid(int fid, IntSet gids) {
		IntList parents = parents(fid);
		for (int i=0; i<parents.size(); i++) {
			int parentFid = parents.getInt(i);
			if (gids.add(gid(parentFid))) {
				addAscendantGidsFromFid(parentFid, gids);
			}
		}
	}


	// -- computing supports ------------------------------------------------------------------------------------------
	
	/** Computes the item frequencies of the items in a sequence (including ancestors).
	 *
	 * @param sequence the input sequence
	 * @param itemCounts stores the result (map from item identifier to frequency)
	 * @param ancItems a temporary set used by this method (for reuse; won't be read but is modified)
	 * @param usesFids whether the input sequence consists of gid or fid item identifiers
	 * @param support the support of the sequence
	 */
	public void computeItemFrequencies(IntCollection sequence, Int2LongMap itemCounts, IntSet ancItems, boolean usesFids, long support) {
		assert itemCounts.defaultReturnValue() <= 0;
		itemCounts.clear();
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
				long oldValue = itemCounts.put(item, support);
				if (oldValue > 0) {
					itemCounts.put(item, oldValue + support);
				}
			}
		}
	}

	/** Updates the counts of the hierarchy by adding the given input sequence. Does
	 * not modify fids, so those may be inconsistent afterwards. <code>itemCounts</code> and
	 * <code>ancItems</code> are temporary variables.
	 */
	public void incCounts(IntCollection inputSequence, Int2LongMap itemCounts, IntSet ancItems, boolean usesFids, int support) {
		computeItemFrequencies(inputSequence, itemCounts, ancItems, usesFids, support);
		for (Int2LongMap.Entry entry : itemCounts.int2LongEntrySet()) {
			int fid = usesFids ? entry.getIntKey() : fid(entry.getIntKey());
			incCounts(fid, support, entry.getLongValue());
		}
		hasValidFids = null;
	}

	/** Updates the counts of the hierarchy by adding the given input sequences. Does
	 * not modify fids, so those may be inconsistent afterwards.  */
	public void incCounts(SequenceReader reader) throws IOException {
		boolean usesFids = reader.usesFids();
		IntList inputSequence = new IntArrayList();
		Int2LongMap itemCounts = new Int2LongOpenHashMap();
		itemCounts.defaultReturnValue(0);
		IntSet ancItems = new IntOpenHashSet();
		while (reader.read(inputSequence)) {
			incCounts(inputSequence, itemCounts, ancItems, usesFids, 1);
		}
	}

	public void incCounts(int fid, long support, long csupport) {
		long temp = supports.getLong(fid);
		supports.set(fid, temp+support);
		temp = cSupports.getLong(fid);
		cSupports.set(fid, temp+csupport);
	}

	/** Merges the provided dictionary (including counts) into this dictionary. Items are matched based on
	 * {@link Item#sid}. The provided dictionary must be consistent with this one in that every item that occurs
	 * in both dictionaries must have matching parents. Item properties and identifiers are retained from this
	 * dictionary if present; otherwise, they are taken from the other dictionary (if possible without conflict).
	 * Fids are likely to be inconsistent after merging and need to be recomputed (see {@link #hasValidFids()}).
	 *
	 * @throws IllegalArgumentException if other is inconsistent with this dictionary (both dictionary should then be
	 *                                  considered destroyed)
	 */
	public void mergeWith(Dictionary other) {
		int maxGid = Collections.max(gids);
		Set<String> thisParentSids = new HashSet<>();
		Set<String> otherParentSids = new HashSet<>();

		for (int otherFid : other.topologicalSort()) {
			String sid = other.sidForFid(otherFid);
			int thisFid = fid(sid);

			if (thisFid < 0) {
				// a new item: copy item from other and retain what's possible to retain
				int gid = other.gid(otherFid);
				if (fid(gid) >= 0) { // we give the other item a new gid
					maxGid++;
					gid = maxGid;
				} else {
					maxGid = Math.max(maxGid, gid);
				}

				int fid = addItem(gid, sid, other.support(otherFid), other.cSupport(otherFid),
						new IntArrayList(other.parents(otherFid).size()),
						new IntArrayList(other.children(otherFid).size()),
						other.properties(otherFid));

				// add parents
				IntList otherParentFids = other.parents(otherFid);
				for (int i=0; i<otherParentFids.size(); i++) {
					int otherParentFid = otherParentFids.get(i);
					int thisParentFid = fid(other.sidForFid(otherParentFid));
					if (thisParentFid == -1) {
						throw new IllegalArgumentException("parents don't match for sid " + sid);
					}
					addParent(fid, thisParentFid);
				}
			} else {
				// check parents
				thisParentSids.clear();
				for (int parentFid : parents(thisFid)) thisParentSids.add(sidForFid(parentFid));
				otherParentSids.clear();
				for (int parentFid : other.parents(otherFid)) otherParentSids.add(other.sidForFid(parentFid));
				if (!thisParentSids.equals(otherParentSids))
					throw new IllegalArgumentException("parents of item sid=" + sidForFid(thisFid) + " disagree");

				// fine; we only need to merge counts
				incCounts(thisFid, other.support(otherFid), other.cSupport(otherFid));
			}
		}
	}


	/** Returns a copy of this dictionary that contains only the specified items (including
	 * the *direct* links between these items).
	 * 
	 * TODO: also add indirect links? (takes some thought to figure out which links to acutally add and how to do this 
	 *       reasonably efficiently. One option: compute transitive closure, drop items to be removed, then compute
	 *       transitive reduction.)
	 */
	public Dictionary restrictedCopy(IntSet itemFids) {
		// copy the relevant items
		Dictionary dict = new Dictionary();
		for (Item item : itemsByFid.values()) {
			if (!itemFids.contains(item.fid)) continue;
			Item copiedItem = item.shallowCopyWithoutEdges();
			dict.addItem(copiedItem);
		}
		
		// add indirect links
		for (Item copiedItem : dict.itemsById.values()) {
			Item item = getItemByFid(copiedItem.fid);
			for (Item child : item.children) {
				if (itemFids.contains(child.fid)) {
					Item copiedChild = dict.getItemByFid(child.fid);
					Item.addParent(copiedChild, copiedItem);
				}
			}
		}

		// compute fid index for restricted dictionary
        dict.indexParentFids();

		return dict;
	}
	

	// -- computing fids --------------------------------------------------------------------------

	/** Recomputes all fids such that (1) items with higher document frequency have lower fid and 
	 * (2) parents have lower fids than their children. Assumes that document frequencies are
	 * consistent (i.e., parents never have lower document frequency than one of their children).
	 */
	public void recomputeFids() {
		// compute a topological sort; also respects document frequencies
		IntList order = topologicalSort();

		// we now want item order(i) at position i
		// set fids and index all items based on sort order
		for (int i=0; i<order.size(); i++) {
			int j=i;
			while (order.get(j) != -1 && order.get(j) != i) {
				swap(i,j);
				order.set(j, -1);
			}
			order.set(i, -1);
		}

		// finally, let's compute the indexes
		gidIndex.clear();
		sidIndex.clear();
		for (int i=0; i<size(); i++) {
			gidIndex.put(gids.getInt(i), i);
			sidIndex.put(sids.get(i), i);
		}

		// remember that the fids are valid
		hasValidFids = true;
	}

	private void swap(int fid1, int fid2) {
		int gid1 = gids.getInt(fid1);
		int gid2 = gids.getInt(fid2);
		gids.set(fid1, gid2);
		gids.set(fid2, gid1);

		String sid1 = sids.get(fid1);
		String sid2 = sids.get(fid2);
		sids.set(fid1, sid2);
		sids.set(fid2, sid1);

		long s1 = supports.getLong(fid1);
		long s2 = supports.getLong(fid2);
		supports.set(fid1, s2);
		supports.set(fid2, s1);

		s1 = cSupports.getLong(fid1);
		s2 = cSupports.getLong(fid2);
		cSupports.set(fid1, s2);
		cSupports.set(fid2, s1);

		IntArrayList l1 = parents.get(fid1);
		IntArrayList l2 = parents.get(fid2);
		parents.set(fid1, l2);
		parents.set(fid2, l1);

		l1 = children.get(fid1);
		l2 = children.get(fid2);
		children.set(fid1, l2);
		children.set(fid2, l1);

		DesqProperties p1 = properties.get(fid1);
		DesqProperties p2 = properties.get(fid2);
		properties.set(fid1, p2);
		properties.set(fid2, p1);
	}

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
    	IntList resultIds = new IntArrayList();
        IntSet visitedIds = new IntOpenHashSet();

        /* We'll also maintain a third set consisting of all nodes that have
         * been fully expanded.  If the graph contains a cycle, then we can
         * detect this by noting that a node has been explored but not fully
         * expanded.
         */
        IntSet expandedIds = new IntOpenHashSet();

        // Sort the all items by decreasing support. This way,
        // items with a higher frequency will always appear before items
        // with lower frequency (under the assumption that document frequencies
        // are valid).
        IntList ids = new IntArrayList(size());
		for (int i=0; i<size(); i++) {
			ids.add(i);
		}
        Collections.sort(ids, (id1,id2) -> Long.compare(supports.getLong(id2), supports.getLong(id1)));

        /* Fire off a DFS from each node in the graph. */
        for (int id=0; i<ids.size(); id++) {
			explore(id, resultIds, visitedIds, expandedIds);
		}

        /* Hand back the resulting ordering. */
        return resultIds;
    }

    /**
     * Recursively performs a DFS from the specified node, marking all nodes
     * encountered by the search.
     *
     * @param id The node to begin the search from.
     * @param orderingIds A list holding the topological sort of the graph.
     * @param visitedIds A set of nodes that have already been visited.
     * @param expandedIds A set of nodes that have been fully expanded.
	 *
     * @author Keith Schwarz (htiek@cs.stanford.edu) 
     */
    private void explore(int id, IntList orderingIds, IntSet visitedIds,
                                    IntSet expandedIds) {
        /* Check whether we've been here before.  If so, we should stop the
         * search.
         */
        if (visitedIds.contains(id)) {
            /* There are two cases to consider.  First, if this node has
             * already been expanded, then it's already been assigned a
             * position in the final topological sort and we don't need to
             * explore it again.  However, if it hasn't been expanded, it means
             * that we've just found a node that is currently being explored,
             * and therefore is part of a cycle.  In that case, we should 
             * report an error.
             */
            if (expandedIds.contains(id)) return;
            throw new IllegalArgumentException("Graph contains a cycle.");
        }
        
        /* Mark that we've been here */
        visitedIds.add(id);

        /* Recursively explore all of the node's predecessors. */
        IntList parents = parents(id);
		for (int i=0; i<parents.size(); i++)
            explore(parents.get(i), orderingIds, visitedIds, expandedIds);

        /* Having explored all of the node's predecessors, we can now add this
         * node to the sorted ordering.
         */
        orderingIds.add(id);

        /* Similarly, mark that this node is done being expanded. */
        expandedIds.add(id);
    }


	// -- I/O ---------------------------------------------------------------------------------------------------------

	public static Dictionary loadFrom(String fileName) throws IOException {
		return loadFrom(new File(fileName));
	}

	public static Dictionary loadFrom(File file) throws IOException {
		Dictionary dict = new Dictionary();
		dict.read(file);
		return dict;
	}

	public static Dictionary loadFrom(URL url) throws IOException {
		Dictionary dict = new Dictionary();
		dict.read(url);
		return dict;
	}

	/** Reads a dictionary from a file. Automatically determines the right format based on file extension. */
	public void read(File file) throws IOException {
		if (file.getName().endsWith(".json")) {
			readJson(new FileInputStream(file));
			return;
		}
		if (file.getName().endsWith(".json.gz")) {
			readJson(new GZIPInputStream(new FileInputStream(file)));
			return;
		}
		if (file.getName().endsWith(".avro")) {
			readAvro(new FileInputStream(file));
			return;
		}
		if (file.getName().endsWith(".avro.gz")) {
			readAvro(new GZIPInputStream(new FileInputStream(file)));
			return;
		}
		throw new IllegalArgumentException("unknown file extension: " + file.getName());
	}

	/** Reads a dictionary from an URL. Automatically determines the right format based on file extension. */
	public void read(URL url) throws IOException {
		if (url.getFile().endsWith(".json")) {
			readJson(url.openStream());
			return;
		}
		if (url.getFile().endsWith(".json.gz")) {
			readJson(new GZIPInputStream(url.openStream()));
			return;
		}
		if (url.getFile().endsWith(".avro")) {
			readAvro(url.openStream());
			return;
		}
		if (url.getFile().endsWith(".avro.gz")) {
			readAvro(new GZIPInputStream(url.openStream()));
			return;
		}
		throw new IllegalArgumentException("unknown file extension: " + url.getFile());
	}

	public void write(String fileName) throws IOException {
		write(new File(fileName));
	}

	public void write(File file) throws IOException {
		if (file.getName().endsWith(".json")) {
			writeJson(new FileOutputStream(file));
			return;
		}
		if (file.getName().endsWith(".json.gz")) {
			writeJson(new GZIPOutputStream(new FileOutputStream(file)));
			return;
		}
		if (file.getName().endsWith(".avro")) {
			writeAvro(new FileOutputStream(file));
			return;
		}
		if (file.getName().endsWith(".avro.gz")) {
			writeAvro(new GZIPOutputStream(new FileOutputStream(file)));
			return;
		}
		throw new IllegalArgumentException("unknown file extension: " + file.getName());
	}

	public AvroItem toAvroItem(int fid, AvroItem avroItem) {
		if (avroItem == null) {
			avroItem = new AvroItem();
		}

		// basic fields
		avroItem.setFid(fid);
		avroItem.setGid(gid(fid));
		avroItem.setSid(sidForFid(fid));
		avroItem.setCFreq(support(fid));
		avroItem.setDFreq(cSupport(fid));
		avroItem.setNoChildren(children(fid).size());

		// parents
		List<Integer> parentGids = avroItem.getParentGids();
		if (parentGids == null) {
			parentGids = new IntArrayList();
		} else {
			parentGids.clear();
		}
		IntList parentFids = parents(fid);
		for (int parentFid=0; parentFid<parentFids.size(); parentFid++) {
			parentGids.add(gid(parentFid));
		}
		avroItem.setParentGids(parentGids);

		// properties
		List<AvroItemProperties> avroItemProperties = avroItem.getProperties();
		if (avroItemProperties == null) {
			avroItemProperties = new ArrayList<>();
		} else {
			avroItemProperties.clear();
		}
		DesqProperties properties = properties(fid);
		if (properties != null) {
			Iterator<String> keysIt = properties.getKeys();
			while (keysIt.hasNext()) {
				String key = keysIt.next();
				String value = properties.getString(key, null);
				avroItemProperties.add(new AvroItemProperties(key, value));
			}
		}
		avroItem.setProperties(avroItemProperties);

		return avroItem;
	}

	public void writeAvro(OutputStream out) throws IOException {
		// set up writers
		AvroItem avroItem = new AvroItem();
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		DataFileWriter<AvroItem> dataFileWriter = new DataFileWriter<>(itemDatumWriter);
		dataFileWriter.create(avroItem.getSchema(), out);

		// write items in topological order
		IntList items = topologicalSort();
		for (int i=0; i<items.size(); i++) {
			int fid = items.get(i);
			avroItem = toAvroItem(fid, avroItem);
			dataFileWriter.append(avroItem);
		}

		// close
		dataFileWriter.close();
	}

	public void readAvro(InputStream in) throws IOException {
		readAvro(in, Integer.MAX_VALUE);
	}

	private void readAvro(InputStream in, int maxItems) throws IOException {
		clear();
		DatumReader<AvroItem> itemDatumReader = new SpecificDatumReader<>(AvroItem.class);
		DataFileStream<AvroItem> dataFileReader = new DataFileStream<>(in, itemDatumReader);
		AvroItem avroItem = null;
		while (dataFileReader.hasNext() && maxItems > 0) {
			maxItems--;
			avroItem = dataFileReader.next(avroItem);
			addItem(avroItem);
		}

		// check that fids are dense
		for (int i=0; i<gids.size(); i++) {
			if (gids.getInt(i) < 0) {
				throw new IOException("fids are not dense (e.g., fid " + i + " is missing)");
			}
		}
	}

	public void writeJson(OutputStream out) throws IOException {
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		List<Integer> items = topologicalSort();
		AvroItem avroItem = new AvroItem();
		Encoder encoder = EncoderFactory.get().jsonEncoder(avroItem.getSchema(), out);
		for (int i=0; i<items.size(); i++) {
			int fid = items.get(i);
			avroItem = toAvroItem(fid, avroItem);
			itemDatumWriter.write( avroItem, encoder );
		}
		encoder.flush();
	}

	public void readJson(InputStream in) throws IOException {
		clear();
		DatumReader<AvroItem> itemDatumReader = new SpecificDatumReader<>(AvroItem.class);
		AvroItem avroItem = new AvroItem();
		Decoder decoder = DecoderFactory.get().jsonDecoder(avroItem.getSchema(), in);
		try {
			while ((avroItem = itemDatumReader.read(avroItem, decoder)) != null) {
				addItem(avroItem);
			}
		} catch (EOFException e) {
			// fine, we read everything
		}

		// check that fids are dense
		for (int i=0; i<gids.size(); i++) {
			if (gids.getInt(i) < 0) {
				throw new IOException("fids are not dense (e.g., fid " + i + " is missing)");
			}
		}
	}

	private byte[] toBytes() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(size()*50);
		writeAvro(bytesOut);
		return bytesOut.toByteArray();
	}

	private static Dictionary fromBytes(byte[] bytes) throws IOException {
		Dictionary dict = new Dictionary();
		dict.readAvro(new ByteArrayInputStream(bytes));
		return dict;
	}

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
		// not very efficient to write, but allows for fast reads
		byte[] bytes = toBytes();
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		readAvro(new DataInput2InputStreamWrapper(in, length));
		// since Avro internally prereads the input stream beyond its actually serialized size, we need to limit
		// the number of bytes to be read above
	}

	public static final class KryoSerializer extends Writable2Serializer<Dictionary> {
		@Override
		public Dictionary newInstance(Kryo kryo, Input input, Class<Dictionary> type) {
			return new Dictionary();
		}
	}
}
