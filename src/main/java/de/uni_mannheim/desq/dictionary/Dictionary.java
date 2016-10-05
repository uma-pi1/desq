package de.uni_mannheim.desq.dictionary;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.avro.AvroItem;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.mining.WeightedSequence;
import de.uni_mannheim.desq.util.IntSetUtils;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** A set of items arranged in a hierarchy */ 
public final class Dictionary implements Externalizable, Writable {
	// indexes
    final Int2ObjectMap<Item> itemsById = new Int2ObjectOpenHashMap<>();
	final Int2ObjectMap<Item> itemsByFid = new Int2ObjectOpenHashMap<>();
	final Map<String, Item> itemsBySid = new HashMap<>();

    /** Index for quickly accessing the fids of the parents of a given item. The parents are stored in range
     * <code>parentFidsOffsets[itemFid]</code> (inclusive) to <code>parentFidsOffsets[itemFid+1]</code> (exclusive).
     */
    public int[] parentFids;

    /** Determines where the parents fids for each item are stored in {@link #parentFids} */
    public int[] parentFidsOffsets;

    /** Whether this dictionary is a forest. See {@link #isForest()}. */
    private boolean isForest = false;

	// -- construction ----------------------------------------------------------------------------

	public Dictionary deepCopy() {
		// this could be made more efficient with more lines of code
		try {
			byte[] bytes = toBytes();
			return Dictionary.fromBytes(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// -- updating the hierarchy ------------------------------------------------------------------
	
	/** Adds a new item to the hierarchy. If the fid equals 0, it won't be indexed */
	public void addItem(Item item) {
		if (containsGid(item.gid)) {
			throw new IllegalArgumentException("Item gid '" + item.gid + "' exists already");
		}
		if (itemsBySid.containsKey(item.sid)) {
			throw new IllegalArgumentException("Item sid '" + item.sid + "' exists already");
		}
		if (item.fid >= 0 && itemsByFid.containsKey(item.fid)) {
			throw new IllegalArgumentException("Item fid '" + item.gid + "' exists already");
		}
		itemsById.put(item.gid, item);
		if (item.fid >= 0) itemsByFid.put(item.fid, item);
		itemsBySid.put(item.sid, item);
	}

	/** Computes the item frequencies of the items in a sequence (including ancestors).
	 *
	 * @param sequence the input sequence
	 * @param itemCounts stores the result (map from item identifier to frequency)
	 * @param ancItems a temporary set used by this method (for reuse; won't be read but is modified)
	 * @param usesFids whether the input sequence consists of gid or fid item identifiers
	 * @param support the support of the sequence
	 */
	public void computeItemFrequencies(IntCollection sequence, Int2IntMap itemCounts, IntSet ancItems, boolean usesFids, int support) {
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
				addAscendantIds(getItemByGid(ii), ancItems);
			}
			for (int item : ancItems) {
				int oldValue = itemCounts.put(item, support);
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
	public void incCounts(IntCollection inputSequence, Int2IntMap itemCounts, IntSet ancItems, boolean usesFids, int support) {
		computeItemFrequencies(inputSequence, itemCounts, ancItems, usesFids, support);
		for (Int2IntMap.Entry entry : itemCounts.int2IntEntrySet()) {
			Item item = usesFids ? getItemByFid(entry.getIntKey()) : getItemByGid(entry.getIntKey());
			item.dFreq += support;
			item.cFreq += entry.getIntValue();
		}
	}

	/** Updates the counts of the hierarchy by adding the given input sequences. Does
	 * not modify fids, so those may be inconsistent afterwards.  */
	public void incCounts(SequenceReader reader) throws IOException {
		boolean usesFids = reader.usesFids();
		IntList inputSequence = new IntArrayList();
		Int2IntOpenHashMap itemCounts = new Int2IntOpenHashMap();
		itemCounts.defaultReturnValue(0);
		IntSet ancItems = new IntOpenHashSet();
		while (reader.read(inputSequence)) {
			incCounts(inputSequence, itemCounts, ancItems, usesFids, 1);
		}
	}

	/** Clears everything */
	public void clear() {
		itemsById.clear();
		itemsByFid.clear();
		itemsBySid.clear();
		parentFids = null;
		parentFidsOffsets = null;
		isForest = false;
	}

	/** Sets fid of all items to -1 */
	public void clearFids() {
		itemsByFid.clear();
		for (Item item : itemsById.values()) {
			item.fid = -1;
		}
	}

	/** Sets fid of all items to -1 and clears all counts */
	public void clearCountsAndFids() {
		itemsByFid.clear();
		for (Item item : itemsById.values()) {
			item.fid = -1;
			item.cFreq = 0;
			item.dFreq = 0;
		}
	}

	/** Merges the provided dictionary (including counts) into this dictionary. Items are matched based on
	 * {@link Item#sid}. The provided dictionary must be consistent with this one in that every item that occurs
	 * in both dictionaries must have matching parents. Item properties and identifiers are retained from this
	 * dictionary if present; otherwise, they are taken from the other dictionary (if possible without conflict).
	 * Fids are likely to be inconsistent after merging and need to be recomputed (see {@link #checkFids()}).
	 *
	 * @throws IllegalArgumentException if other is inconsistent with this dictionary (both dictionary should then be
	 *                                  considered destroyed)
	 */
	public void mergeWith(Dictionary other) {
		int maxGid = Collections.max(itemsById.keySet());
		Set<String> thisParentSids = new HashSet<>();
		Set<String> otherParentSids = new HashSet<>();

		for (int otherItemGid : other.topologicalSort()) {
			Item otherItem = other.getItemByGid(otherItemGid);
			Item thisItem = getItemBySid(otherItem.sid);

			if (thisItem == null) {
				// a new item: copy item from other and retain what's possible to retain
				int gid = otherItem.gid;
				if (containsGid(otherItem.gid)) { // we give the other item a new gid
					maxGid++;
					gid = maxGid;
				} else {
					maxGid = Math.max(maxGid, gid);
				}
				Item newItem = new Item(gid, otherItem.sid);
				newItem.cFreq = otherItem.cFreq;
				newItem.dFreq = otherItem.dFreq;
				newItem.properties = otherItem.properties;

				// add parents
				for (Item parent : otherItem.parents) {
					Item.addParent(newItem, getItemBySid(parent.sid)); // throws exception if parent not present here, as desired
				}

				// add item
				addItem(newItem);
			} else {
				// check parents
				thisParentSids.clear();
				for (Item p : thisItem.parents) thisParentSids.add(p.sid);
				otherParentSids.clear();
				for (Item p : otherItem.parents) otherParentSids.add(p.sid);
				if (!thisParentSids.equals(otherParentSids))
					throw new IllegalArgumentException("parents of item sid=" + thisItem.sid + " disagree");

				// fine; we only need to merge counts
				thisItem.cFreq += otherItem.cFreq;
				thisItem.dFreq += otherItem.dFreq;
			}
		}
	}

	// -- access to indexes -----------------------------------------------------------------------
	
	public Collection<Item> getItems() {
		return itemsById.values();
	}

	public Int2ObjectMap<Item> getItemsByFid() { return itemsByFid; }

	/** Checks whether there is an item with the given ID in the hierarchy. */
	public boolean containsGid(int itemId) {
		return itemsById.containsKey(itemId);
	}
	
	/** Returns the item with the given gid (or null if no such item exists) */
	public Item getItemByGid(int itemId) {
		return itemsById.get(itemId);
	}
	/** Returns all items for the given fids */
	public List<Item> getItemsByIds(IntCollection itemIds) {
		List<Item> items = new ArrayList<>();
		getItemsByIds(itemIds, items);
		return items;
	}

	/** Stores all items for the given fids in the target list */
	public void getItemsByIds(IntCollection itemFids, List<Item> target) {
		target.clear();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			target.add(getItemByGid(it.nextInt()));
		}		
	}

	/** Checks whether there is an item with the given FID in the hierarchy. */
	public boolean containsFid(int itemFid) {
		return itemsByFid.containsKey(itemFid);
	}
	
	/** Returns the item with the given fid (or null if no such item exists) */
	public Item getItemByFid(int itemFid) {
		return itemsByFid.get(itemFid);
	}
	
	/** Returns all items for the given fids */
	public List<Item> getItemsByFids(IntCollection itemFids) {
		List<Item> items = new ArrayList<>();
		getItemsByFids(itemFids, items);
		return items;
	}
	
	/** Stores all items for the given fids in the target list */
	public void getItemsByFids(IntCollection itemFids, List<Item> target) {
		target.clear();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			target.add(getItemByFid(it.nextInt()));
		}		
	}
	
	/** Checks whether there is an item with the given sid in the hierarchy. */
	public boolean containsSid(String itemSid) {
		return itemsBySid.containsKey(itemSid);
	}
	
	/** Returns the item with the given sid (or null if no such item exists) */
	public Item getItemBySid(String itemSid) {
		return itemsBySid.get(itemSid);
	}
	
	
	// -- computing descendants and ascendants ----------------------------------------------------

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
	public IntSet descendantsFids(int itemFid) {
		return descendantsFids(IntSets.singleton(itemFid));		
	}
	
	/** Returns the fids of all descendants of the given items (including the given items) */
	public IntSet descendantsFids(IntCollection itemFids) {
		IntSet descendants = new IntOpenHashSet();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			int itemFid = it.nextInt();
			if (descendants.add(itemFid)) {
				addDescendantFids(getItemByFid(itemFid), descendants);
			}
		}
		return IntSetUtils.optimize(descendants);
	}
	
	/** Adds all descendants of the specified item to itemFids, excluding the given item and all 
	 * descendants of items already present in itemFids. */	
	public void addDescendantFids(Item item, IntSet itemFids) {
		for (Item child : item.children) {
			if (itemFids.add(child.fid)) {
				addDescendantFids(getItemByFid(child.fid), itemFids);
			}
		}
	}

	/** Returns the fids of all ascendants of the given item (including the given item) */
	public IntSet ascendantsFids(int itemFid) {
		return ascendantsFids(IntSets.singleton(itemFid));		
	}

	/** Returns the fids of all ascendants of the given items (including the given items) */
	public IntSet ascendantsFids(IntCollection itemFids) {
		IntSet ascendants = new IntOpenHashSet();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			int itemFid = it.nextInt();
			if (ascendants.add(itemFid)) {
				addAscendantFids(itemFid, ascendants);
			}
		}
		return ascendants;
	}
	
	/** Adds all ascendants of the specified item to itemFids, excluding the given item and all 
	 * ascendants of items already present in itemFids. */
	public void addAscendantFids(Item item, IntSet itemFids) {
        for (Item parent : item.parents) {
            if (itemFids.add(parent.fid)) {
                addAscendantFids(getItemByFid(parent.fid), itemFids);
            }
        }
    }

    /** Adds all ascendants of the specified item to itemFids, excluding the given item and all
     * ascendants of items already present in itemFids. This method is performance-critical for many mining methods.
	 * Best performance is achieved if itemFids is as obtained from {@link #newFidCollection()}.
	 *
     * If the dictionary does not form a forest (see {@link #isForest()}) or if <code>itemFids</code> is not initially
     * empty, <code>itemFids</code> *must* be an {@link IntSet}, ideally one that allows for fast look-ups
     * (such as {@link IntOpenHashSet} or{@link IntAVLTreeSet}). Otherwise, for best performance, pass an
     * {@link IntArrayList}.
     */
    public final void addAscendantFids(int itemFid, IntCollection itemFids) {
        assert itemFids instanceof IntSet || (isForest() && itemFids.isEmpty());
        if (itemFids.isEmpty())
            addAscendantFids(itemFid, itemFids, Integer.MAX_VALUE);
        else
            addAscendantFids(itemFid, itemFids, Integer.MIN_VALUE);
    }

    // minFidInItemFids is a lowerbound of the smallest fid currently in the set. This method makes use of the
    // fact that parents have smaller fids than their children.
    private int addAscendantFids(int itemFid, IntCollection itemFids, int minFidInItemFids) {
        int from = parentFidsOffsets[itemFid];
        int to = parentFidsOffsets[itemFid+1];
        for (int i=from; i<to; i++) {
            int parentFid = parentFids[i];
            if (parentFid < minFidInItemFids) { // must be new
                itemFids.add(parentFid);
                minFidInItemFids = parentFid;
                minFidInItemFids = addAscendantFids(parentFid, itemFids, minFidInItemFids);
            } else if (itemFids.add(parentFid)) { // else look-up and ignore if present
                assert itemFids instanceof IntSet;
                minFidInItemFids = addAscendantFids(parentFid, itemFids, minFidInItemFids);
            }
        }
        return minFidInItemFids;
    }

	/** Adds all ascendants of the specified item to itemFids, excluding the given item and all 
	 * ascendants of items already present in itemFids. */
	public void addAscendantIds(Item item, IntSet itemIds) {
		for (Item parent : item.parents) {
			if (itemIds.add(parent.gid)) {
				addAscendantIds(getItemByGid(parent.gid), itemIds);
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
	
	// -- utility methods -------------------------------------------------------------------------
	
	/** Gets the largest fid of in item with document frequency at least as large as specified. Returns -1 if
	 * there is no such item.
     */
	public int getLargestFidAboveDfreq(long dFreq) {
		int resultFid = -1;
		for (Entry<Integer, Item> entry : itemsByFid.entrySet()) {
			int fid = entry.getKey();
			Item item = entry.getValue();
			if (fid>resultFid && item.dFreq >= dFreq)
				resultFid = fid;
		}
		return resultFid;
	}

	/** Determines whether the items in this dictionary form a forest. The result of this method is undefined unless
     * {@link #indexParentFids()} has been called. */
	public boolean isForest() {
		return isForest;
	}

	/** in-place */
	public void gidsToFids(IntList items) {
		for (int i=0; i<items.size(); i++) {
			int gid = items.getInt(i);
			int fid = getItemByGid(gid).fid;
			items.set(i, fid);
		}
	}

	public void gidsToFids(IntList ids, IntList fids) {
		fids.size(0);
		for (int i=0; i<ids.size(); i++) {
			int gid = ids.getInt(i);
			int fid = getItemByGid(gid).fid;
			fids.add(fid);
		}
	}

	/** in-place */
	public void fidsToGids(IntList items) {
		for (int i=0; i<items.size(); i++) {
			int fid = items.getInt(i);
			int gid = getItemByFid(fid).gid;
			items.set(i, gid);
		}
	}

	public void fidsToGids(IntList fids, IntList ids) {
		ids.size(0);
		for (int i=0; i<fids.size(); i++) {
			int fid = fids.getInt(i);
			int gid = getItemByFid(fid).gid;
			ids.add(gid);
		}
	}
	
	// -- computing fids --------------------------------------------------------------------------

	/** Recomputes all fids such that (1) items with higher document frequency have lower fid and 
	 * (2) parents have lower fids than their children. Assumes that document frequencies are
	 * consistent (i.e., parents never have lower document frequency than one of their children).
	 */
	public void recomputeFids() {
		// compute a topological sort; also respects document frequencies
		itemsByFid.clear();
		IntList order = topologicalSort();
		
		// set fids and index all items based on sort order
		for (int i=0; i<order.size(); i++) {
			int fid = i+1;
			int id = order.get(i);
			Item item = getItemByGid(id);
			item.fid = fid;
			itemsByFid.put(fid, item);
		}

		indexParentFids();
	}

	/** Checks if fids are assigned and indexed correctly. (If not, use {@link #recomputeFids()}.) */
	public boolean checkFids() {
		if (itemsByFid.size() != itemsById.size()) return false;

		List<Item> items = new ArrayList<>(getItems());
		Collections.sort(items, (i1, i2) -> i1.fid-i2.fid );

		int lastFid = Integer.MIN_VALUE;
		int lastDFreq = Integer.MAX_VALUE;
		for (Item item : items) {
			if (lastFid == item.fid || item.dFreq > lastDFreq) return false;
			lastFid = item.fid;
			lastDFreq = item.dFreq;

			for (Item parent : item.parents) {
				if (parent.fid <= item.fid) return false;
			}
		}

		return true;
	}

	/** Performs a topological sort of the items in this dictionary, respecting document 
	 * frequencies. Throws an IllegalArgumentException if there is a cycle.
	 *
	 * @returns item gids in topological order
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

        // Sort the all items by decreasing document frequency. This way,
        // items with a higher frequency will always appear before items
        // with lower frequency (under the assumption that document frequencies
        // are valid).
        List<Item> items = new ArrayList<>(getItems());
        Collections.sort(items, Item.dfreqDecrComparator());

        /* Fire off a DFS from each node in the graph. */
        for (Item item : items )
            explore(item, resultIds, visitedIds, expandedIds);

        /* Hand back the resulting ordering. */
        return resultIds;
    }

    /**
     * Recursively performs a DFS from the specified node, marking all nodes
     * encountered by the search.
     *
     * @param item The node to begin the search from.
     * @param orderingIds A list holding the topological sort of the graph.
     * @param visitedIds A set of nodes that have already been visited.
     * @param expandedIds A set of nodes that have been fully expanded.
	 *
     * @author Keith Schwarz (htiek@cs.stanford.edu) 
     */
    private void explore(Item item, IntList orderingIds, IntSet visitedIds,
                                    IntSet expandedIds) {
        /* Check whether we've been here before.  If so, we should stop the
         * search.
         */
        if (visitedIds.contains(item.gid)) {
            /* There are two cases to consider.  First, if this node has
             * already been expanded, then it's already been assigned a
             * position in the final topological sort and we don't need to
             * explore it again.  However, if it hasn't been expanded, it means
             * that we've just found a node that is currently being explored,
             * and therefore is part of a cycle.  In that case, we should 
             * report an error.
             */
            if (expandedIds.contains(item.gid)) return;
            throw new IllegalArgumentException("Graph contains a cycle.");
        }
        
        /* Mark that we've been here */
        visitedIds.add(item.gid);

        /* Recursively explore all of the node's predecessors. */
        for (Item predecessor: item.parents)
            explore(predecessor, orderingIds, visitedIds, expandedIds);

        /* Having explored all of the node's predecessors, we can now add this
         * node to the sorted ordering.
         */
        orderingIds.add(item.gid);

        /* Similarly, mark that this node is done being expanded. */
        expandedIds.add(item.gid);
    }

    /** Indexes the parents of each item. Needs to be called before using
     * {@link #addAscendantFids(int, IntCollection)}. Note that this method is implicitly called when recomputing fids
     * via {@link #recomputeFids()} and when loading a dictionary with fids from some file. */
    public void indexParentFids() {
        isForest = true;
        IntArrayList fids = new IntArrayList(itemsByFid.keySet());
        Collections.sort(fids);
        IntList tempParentFids = new IntArrayList();
        parentFidsOffsets = new int[fids.getInt(fids.size()-1)+2];
        int offset = 0;
        int prevFid = 0;
        for (int fid : fids) {
            // update left out fids (no parents)
            for (prevFid++; prevFid < fid; prevFid++) {
                parentFidsOffsets[prevFid+1] = parentFidsOffsets[prevFid];
            }
            assert prevFid == fid;

            // check whether we still have a forest (assuming a valid DAG)
            Item item = getItemByFid(fid);
            if (item.parents.size()>1) isForest = false;

            // update next fid
            for (Item parent : item.parents) {
                tempParentFids.add(parent.fid);
                offset++;
            }
            parentFidsOffsets[fid+1] = offset;
        }

        // update remaining fids (no parents)
        for (prevFid++; prevFid < parentFidsOffsets.length-1; prevFid++) {
            parentFidsOffsets[prevFid+1] = parentFidsOffsets[prevFid];
        }

        parentFids = tempParentFids.toIntArray();
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

	public void writeAvro(OutputStream out) throws IOException {
		// set up writers
		AvroItem avroItem = new AvroItem();
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		DataFileWriter<AvroItem> dataFileWriter = new DataFileWriter<>(itemDatumWriter);
		dataFileWriter.create(avroItem.getSchema(), out);

		// write items in topological order
		List<Integer> items = topologicalSort();
		for (int itemGid : items) {
			dataFileWriter.append( getItemByGid(itemGid).toAvroItem(avroItem) );
		}

		// close
		dataFileWriter.close();
	}

	public void readAvro(InputStream in) throws IOException {
		clear();
		DatumReader<AvroItem> itemDatumReader = new SpecificDatumReader<>(AvroItem.class);
		DataFileStream<AvroItem> dataFileReader = new DataFileStream<>(in, itemDatumReader);
		AvroItem avroItem = null;
		boolean hasFids = true;
		while (dataFileReader.hasNext()) {
			avroItem = dataFileReader.next(avroItem);
			Item item = Item.fromAvroItem(avroItem, this);
			if (item.fid < 0) {
				hasFids = false;
			}
			addItem(item);
		}
		if (hasFids) {
			indexParentFids();
		}
	}

	public void writeJson(OutputStream out) throws IOException {
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		List<Integer> items = topologicalSort();
		AvroItem avroItem = new AvroItem();
		Encoder encoder = EncoderFactory.get().jsonEncoder(avroItem.getSchema(), out);
		for (int itemGid : items) {
			itemDatumWriter.write( getItemByGid(itemGid).toAvroItem(avroItem), encoder );
		}
		encoder.flush();
	}

	public void readJson(InputStream in) throws IOException {
		clear();
		DatumReader<AvroItem> itemDatumReader = new SpecificDatumReader<>(AvroItem.class);
		AvroItem avroItem = new AvroItem();
		Decoder decoder = DecoderFactory.get().jsonDecoder(avroItem.getSchema(), in);
		boolean hasFids = true;
		try {
			while ((avroItem = itemDatumReader.read(avroItem, decoder)) != null) {
				Item item = Item.fromAvroItem(avroItem, this);
				if (item.fid < 0) {
					hasFids = false;
				}
				addItem(item);
			}
		} catch (EOFException e) {
			// fine, we read everything
		}
		if (hasFids) {
			indexParentFids();
		}
	}

	public byte[] toBytes() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		writeAvro(bytesOut);
		return bytesOut.toByteArray();
	}

	public static Dictionary fromBytes(byte[] bytes) throws IOException {
		// TODO: this method is quite slow (readAvro is slow for some reason) and may become a bottleneck
		// TODO: for internal use (e.g., broadcasting dictionaries), we may want to switch to a custom binary format
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
		byte[] bytes = toBytes();
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		byte[] bytes = new byte[length];
		in.readFully(bytes);
		readAvro(new ByteArrayInputStream(bytes));
	}

	public static final class KryoSerializer extends Writable2Serializer<Dictionary> {
		@Override
		public Dictionary newInstance(Kryo kryo, Input input, Class<Dictionary> type) {
			return new Dictionary();
		}
	}
}
