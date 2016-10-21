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

/** A set of items arranged in a hierarchy.
 *
 * Each item is associated with two stable unique identifiers: its gid (non-negative integer) and its sid (string). These
 * identifiers are set by the underlying application and do not need to fulfil any requirements (and, in particular,
 * they do not need to be dense).
 *
 * Each item is also associated with an internal positive numeric identifier, its fid. The fid is not stable and may change
 * when the dictionary is changed. Generally, fids should be dense. For many mining tasks, they also need to
 * satisfy additional properties; see {@link #hasConsistentFids}.
*/
public class Dictionary implements Externalizable, Writable {

	// -- information about items -------------------------------------------------------------------------------------

	/** The number of items in this dictionary. */
	protected int size = 0;

	/** Stable global identifiers of this dictionary's items. Indexed by fid; -1 if fid not present.  */
	protected final IntArrayList gids;

	/** Stable unique item names of this dictionary's items. Indexed by fid; null if fid not present. */
	protected final ArrayList<String> sids;

	/** The document frequencies of this dictionary's items. Indexed by fid; -1 if fid not present. */
	protected final LongArrayList dfreqs;

	/** The collection frequencies of this dictionary's items. Indexed by fid; -1 if fid not present. */
	protected final LongArrayList cfreqs;

	/** The fids of the parents of this dictionary's items. Indexed by fid; null if fid not present. */
	protected final ArrayList<IntArrayList> parents;

	/** The fids of the children of this dictionary's items. Indexed by fid; null if fid not present. */
	protected final ArrayList<IntArrayList> children;

	/** Properties associated with this dictionary's items. Indexed by fid; null if fid not present. */
	protected final ArrayList<DesqProperties> properties;


	// -- indexes -----------------------------------------------------------------------------------------------------

	/** Maps gids to fids. */
	protected final Int2IntOpenHashMap gidIndex;

	/** Maps sids to fids. */
	protected final Object2IntOpenHashMap<String> sidIndex;


	// -- information about this dictionary ---------------------------------------------------------------------------

	/** Whether this dictionary is a forest or <code>null</code> if unknown. See {@link #isForest()}. */
	protected Boolean isForest = null;

	/** Whether the fids in this dictionary are consistent or <code>null</code> if unknown. See {@link #hasConsistentFids()}. */
	protected Boolean hasConsistentFids = null;


	// -- construction and modification -------------------------------------------------------------------------------

	public Dictionary() {
		gids = new IntArrayList();
		sids = new ArrayList<>();
		dfreqs = new LongArrayList();
		cfreqs = new LongArrayList();
		parents = new ArrayList<>();
		children = new ArrayList<>();
		properties = new ArrayList<>();
		gidIndex = new Int2IntOpenHashMap();
		gidIndex.defaultReturnValue(-1);
		sidIndex = new Object2IntOpenHashMap<>();
		sidIndex.defaultReturnValue(-1);
	}

	/** Creates a new dictionary backed by given dictionary, except parents and children. Used
	 * for {@link RestrictedDictionary}. */
	protected Dictionary(Dictionary dict) {
		gids = dict.gids;
		sids = dict.sids;
		dfreqs = dict.dfreqs;
		cfreqs = dict.cfreqs;
		parents = new ArrayList<>();
		children = new ArrayList<>();
		properties = dict.properties;
		gidIndex = dict.gidIndex;
		sidIndex = dict.sidIndex;
	}

	/** Always false. Subclasses may override. */
	public boolean isReadOnly() {
		return false;
	}

	protected void ensureWritable() {
		if (isReadOnly())
			throw new UnsupportedOperationException("dictionary is read only");
	}

	/** Adds a new item to this dictionary. */
	public void addItem(int fid, int gid, String sid, long dfreq, long cfreq,
					   IntArrayList parents, IntArrayList children, DesqProperties properties) {
		ensureWritable();
		if (fid <= 0)
			throw new IllegalArgumentException("fid '" + fid + "' is not positive");
		if (containsFid(fid))
			throw new IllegalArgumentException("Item fid '" + fid + "' exists already");
		if (gid < 0)
			throw new IllegalArgumentException("gid '" + gid + "' is negative");
		if (containsGid(gid))
			throw new IllegalArgumentException("Item gid '" + gid + "' exists already");
		if (sid == null)
			throw new IllegalArgumentException("sid for item with '" + gid + "' is null");
		if (containsSid(sid))
			throw new IllegalArgumentException("Item sid '" + sid + "' exists already");

		// create enough space by inserting dummy values
		while (gids.size() <= fid) {
			gids.add(-1);
			sids.add(null);
			dfreqs.add(-1);
			cfreqs.add(-1);
			this.parents.add(null);
			this.children.add(null);
			this.properties.add(null);
		}

		// now put the item
		gids.set(fid, gid);
		sids.set(fid, sid);
		dfreqs.set(fid, dfreq);
		cfreqs.set(fid, cfreq);
		this.parents.set(fid, parents);
		this.children.set(fid, children);
		this.properties.set(fid, properties);
		gidIndex.put(gid, fid);
		sidIndex.put(sid, fid);

		// update cached information
		size += 1;
		isForest = null;
		hasConsistentFids = null;
	}

	/** Adds an item and returns the fid assigned to the added item. */
	public int addItem(int gid, String sid, long dfreq, long cfreq,
					   IntArrayList parents, IntArrayList children, DesqProperties properties) {
		int fid = lastFid()+1;
		if (fid == 0) fid=1;
		addItem(fid, gid, sid, dfreq, cfreq, parents, children, properties);
		return fid;
	}

	/** Adds childFid as a descendant of parentFid. No duplicate checking is performed. */
	public void addParent(int childFid, int parentFid) {
		ensureWritable();
		if (!containsFid(childFid) || !containsFid(parentFid))
			throw new IllegalArgumentException();
		parentsOf(childFid).add(parentFid);
		childrenOf(parentFid).add(childFid);
	}

	/** Adds a new item to this dictionary. */
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
		addItem(
				avroItem.getFid(),
				avroItem.getGid(),
				avroItem.getSid(),
				avroItem.getDfreq(),
				avroItem.getCfreq(),
				new IntArrayList(noParents),
				new IntArrayList(noChildren),
				properties
		);

		// and add parents
		int fid = avroItem.getFid();
		for (Integer parentGid : avroItem.getParentGids()) {
			int parentFid = fidOf(parentGid);
			if (parentFid == -1) {
				throw new RuntimeException("parent item with gid=" + parentGid + " not present");
			}
			addParent(fid, parentFid);
		}
	}

	/** Adds an item and returns the (preliminary) fid assigned to the added item. */
	public void addItem(int fid, int gid, String sid, long dfreq, long cfreq) {
		addItem(fid, gid, sid, dfreq, cfreq, new IntArrayList(1), new IntArrayList(0), null);
	}

	/** Adds an item and returns the (preliminary) fid assigned to the added item. */
	public void addItem(int fid, int gid, String sid) {
		addItem(fid, gid, sid, 0L, 0L);
	}

	/** Adds an item and returns the (preliminary) fid assigned to the added item. */
	public int addItem(int gid, String sid, DesqProperties properties) {
		return addItem(gid, sid, 0L, 0L, new IntArrayList(1), new IntArrayList(0), properties);
	}

	/** Adds an item and returns the (preliminary) fid assigned to the added item. */
	public int addItem(int gid, String sid) {
		return addItem(gid, sid, null);
	}

	/** Clears everything */
	public void clear() {
		ensureWritable();
		size = 0;
		gids.clear();
		sids.clear();
		dfreqs.clear();
		cfreqs.clear();
		parents.clear();
		children.clear();
		properties.clear();
		gidIndex.clear();
		sidIndex.clear();
		isForest = null;
		hasConsistentFids = null;
	}

	/** Clears item frequencies */
	public void clearFreqs() {
		ensureWritable();
		for (int i=0; i<size(); i++) {
			dfreqs.set(i, 0L);
			cfreqs.set(i, 0L);
			// isForest and hasConsistentFids can remain unchanged
		}
	}

	/** Ensures sufficent storage for capacity items */
	public void ensureCapacity(int capacity) {
		ensureWritable();
		gids.ensureCapacity(capacity+1);
		sids.ensureCapacity(capacity+1);
		dfreqs.ensureCapacity(capacity+1);
		cfreqs.ensureCapacity(capacity+1);
		parents.ensureCapacity(capacity+1);
		children.ensureCapacity(capacity+1);
		properties.ensureCapacity(capacity+1);
		// unfortunately, fastutils doesn't allow us to specify the capacity for its hash maps
	}

	/** Reduces the memory footprint of this dictionary as much as possible. */
	public void trim() {
		int newSize = lastFid()+1;
		trim(gids, newSize);
		trim(sids, newSize);
		trim(dfreqs, newSize);
		trim(cfreqs, newSize);
		trim(parents, newSize);
		trim(children, newSize);
		trim(properties, newSize);
		gidIndex.trim();
		sidIndex.trim();

		for (int fid=firstFid(); fid>=0; fid=nextFid(fid)) {
			parentsOf(fid).trim();
			childrenOf(fid).trim();
		}
	}

	private static <T> void trim(ArrayList<T> l, int newSize) {
		while (l.size() > newSize) {
			l.remove(l.size()-1);
		}
		l.trimToSize();
	}

	private static void trim(IntArrayList l, int newSize) {
		l.size(newSize);
		l.trim();
	}

	private static void trim(LongArrayList l, int newSize) {
		l.size(newSize);
		l.trim();
	}

	/** Returns a copy of this dictionary. */
	public Dictionary deepCopy() {
		// TODO: this could be made more efficient with more lines of code
		try {
			byte[] bytes = toBytes();
			return Dictionary.fromBytes(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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

	public boolean containsSid(String sid) {
		return sidIndex.containsKey(sid);
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

	/** Returns the fid for the specified gid or -1 if not present */
	public int fidOf(int gid) {
		return gidIndex.get(gid);
	}

	/** Returns the fid for the specified sid or -1 if not present */
	public int fidOf(String sid) {
		return sidIndex.getInt(sid);
	}

	/** Returns the gid for the specified fid or -1 if not present */
	public int gidOf(int fid) {
		return fid<gids.size() ? gids.getInt(fid) : -1;
	}

	/** Returns the gid for the specified sid or -1 if not present */
	public int gidOf(String sid) {
		int fid = fidOf(sid);
		return fid < 0 ? fid : gidOf(fid);
	}

	/** Returns the sid for the specified fid or null if not present */
	public String sidOfFid(int fid) {
		return fid<sids.size() ? sids.get(fid) : null;
	}

	/** Returns the sids for the specified fids (or null if not present) */
	public List<String> sidsOfFids(IntCollection fids) {
		List<String> result = new ArrayList<>(fids.size());
		IntIterator it = fids.iterator();
		while (it.hasNext())  {
			int fid = it.nextInt();
			result.add(sidOfFid(fid));
		}
		return result;
	}

	/** Returns the sids for the specified gids (or null if not present) */
	public List<String> sidsOfGids(IntCollection gids) {
		List<String> result = new ArrayList<>(gids.size());
		IntIterator it = gids.iterator();
		while (it.hasNext())  {
			int gid = it.nextInt();
			result.add(sidOfGid(gid));
		}
		return result;
	}

	/** Returns all fids. */
	public IntCollection fids() {
		return gidIndex.values();
	}

	/** Returns all gids. */
	public IntCollection gids() {
		return gidIndex.keySet();
	}

	/** Returns all sids. */
	public Set<String> sids() {
		return sidIndex.keySet();
	}

	/** Returns the sid for the specified gid or null if not present */
	public String sidOfGid(int gid) {
		int fid = fidOf(gid);
		return fid < 0 ? null : sidOfFid(fid);
	}

	/** Returns the document frequency of the specified fid or -1 if not present */
	public long dfreqOf(int fid) {
		return dfreqs.getLong(fid);
	}

	public void setDfreqOf(int fid, long dfreq) {
		ensureWritable();
		dfreqs.set(fid, dfreq);
		hasConsistentFids = null;
	}

	public void setCfreqOf(int fid, long cfreq) {
		ensureWritable();
		cfreqs.set(fid, cfreq);
	}

	/** Returns the collection frequency of the specified fid or -1 if not present */
	public long cfreqOf(int fid) {
		return cfreqs.getLong(fid);
	}

	/** Returns the fids of the children of the specified fid or -1 if not present */
	public IntArrayList childrenOf(int fid) {
		return children.get(fid);
	}

	/** Returns the fids of the parents of the specified fid or -1 if not present */
	public IntArrayList parentsOf(int fid) {
		return parents.get(fid);
	}

	/** Returns the properties of the specified fid (can be null) or null if not present */
	public DesqProperties propertiesOf(int fid) {
		return properties.get(fid);
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
	 * If this method returns false, use {@link #recomputeFids()} to change the fids accordingly.
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

	/** Updates the item frequencies in this dictionary by adding weight copies of the given input sequence. Does
	 * not modify fids, so those may be inconsistent afterwards. <code>itemCounts</code> and
	 * <code>ancItems</code> are temporary variables.
	 */
	public void incFreqs(IntCollection inputSequence, Int2LongMap itemCfreqs, IntSet ancItems, boolean usesFids, long weight) {
		ensureWritable();
		computeItemCfreqs(inputSequence, itemCfreqs, ancItems, usesFids, weight);
		for (Int2LongMap.Entry entry : itemCfreqs.int2LongEntrySet()) {
			int fid = usesFids ? entry.getIntKey() : fidOf(entry.getIntKey());
			incFreqs(fid, weight, entry.getLongValue());
		}
		hasConsistentFids = null;
	}

	/** Updates the frequencies in this dictionary adding the given input sequences. Does
	 * not modify fids, so those may be inconsistent afterwards.  */
	public void incFreqs(SequenceReader reader) throws IOException {
		boolean usesFids = reader.usesFids();
		IntList inputSequence = new IntArrayList();
		Int2LongMap itemCounts = new Int2LongOpenHashMap();
		itemCounts.defaultReturnValue(0);
		IntSet ancItems = new IntOpenHashSet();
		while (reader.read(inputSequence)) {
			incFreqs(inputSequence, itemCounts, ancItems, usesFids, 1);
		}
	}

	public void incFreqs(int fid, long dfreq, long cfreq) {
		ensureWritable();
		long temp = dfreqs.getLong(fid);
		dfreqs.set(fid, temp+dfreq);
		temp = cfreqs.getLong(fid);
		cfreqs.set(fid, temp+cfreq);
	}

	/** Merges the provided dictionary (including counts) into this dictionary. Items are matched based on
	 * their sids. The provided dictionary must be consistent with this one in that every item that occurs
	 * in both dictionaries must have matching parents. Item properties and identifiers are retained from this
	 * dictionary if present; otherwise, they are taken from the other dictionary (if possible without conflict).
	 * Fids are likely to be inconsistent after merging and need to be recomputed (see {@link #hasConsistentFids()}).
	 *
	 * @throws IllegalArgumentException if other is inconsistent with this dictionary (both dictionary should then be
	 *                                  considered destroyed)
	 */
	public void mergeWith(Dictionary other) {
		ensureWritable();
		int maxGid = Collections.max(gids);
		Set<String> thisParentSids = new HashSet<>();
		Set<String> otherParentSids = new HashSet<>();

		for (int otherFid : other.topologicalSort()) {
			String sid = other.sidOfFid(otherFid);
			int thisFid = fidOf(sid);

			if (thisFid < 0) {
				// a new item: copy item from other and retain what's possible to retain
				int gid = other.gidOf(otherFid);
				if (fidOf(gid) >= 0) { // we give the other item a new gid
					maxGid++;
					gid = maxGid;
				} else {
					maxGid = Math.max(maxGid, gid);
				}

				int fid = addItem(gid, sid, other.dfreqOf(otherFid), other.cfreqOf(otherFid),
						new IntArrayList(other.parentsOf(otherFid).size()),
						new IntArrayList(other.childrenOf(otherFid).size()),
						other.propertiesOf(otherFid));

				// add parents
				IntList otherParentFids = other.parentsOf(otherFid);
				for (int i=0; i<otherParentFids.size(); i++) {
					int otherParentFid = otherParentFids.getInt(i);
					int thisParentFid = fidOf(other.sidOfFid(otherParentFid));
					if (thisParentFid == -1) {
						throw new IllegalArgumentException("parents don't match for sid " + sid);
					}
					addParent(fid, thisParentFid);
				}
			} else {
				// check parents
				thisParentSids.clear();
				for (int parentFid : parentsOf(thisFid)) thisParentSids.add(sidOfFid(parentFid));
				otherParentSids.clear();
				for (int parentFid : other.parentsOf(otherFid)) otherParentSids.add(other.sidOfFid(parentFid));
				if (!thisParentSids.equals(otherParentSids))
					throw new IllegalArgumentException("parents of item sid=" + sidOfFid(thisFid) + " disagree");

				// fine; we only need to merge counts
				incFreqs(thisFid, other.dfreqOf(otherFid), other.cfreqOf(otherFid));
			}
		}
	}


	// -- computing fids --------------------------------------------------------------------------

	/** Recomputes all fids such that (1) items with higher document frequency have lower fid and 
	 * (2) parents have lower fids than their children. Assumes that document frequencies are
	 * consistent (i.e., parents never have lower document frequency than one of their children).
	 */
	public void recomputeFids() {
		ensureWritable();
		// compute a topological sort; also respects document frequencies
		IntList oldFidOf = topologicalSort();

		// compute a list that lets us get the new fid -1 for each old fid
		IntList newFidOf = new IntArrayList();
		newFidOf.size(gids.size());
		for (int i=0; i<oldFidOf.size(); i++) {
			newFidOf.set(oldFidOf.getInt(i)-1, i+1);
		}

		// we now want item oldFidOf(i-1) at position i
		// set fids and index all items based on sort order
		for (int firstPos=1; firstPos<=oldFidOf.size(); firstPos++) {
			int pos = firstPos;
			int swapPos = oldFidOf.getInt(pos-1);
			if (swapPos == -1) continue;
			while (pos != swapPos && swapPos != firstPos) {
				swap(pos, swapPos); // now pos has the right item
				oldFidOf.set(pos-1, -1);
				pos = newFidOf.getInt(pos-1);

			}
			oldFidOf.set(pos-1, -1);
		}


		// finally, let's compute the indexes and children/parents
		gidIndex.clear();
		sidIndex.clear();
		for (int i=1; i<=size(); i++) {
			assert containsFid(i); // because all valid items must now be at beginning
			IntList l = children.get(i);
			for (int j=0; j<l.size(); j++) {
				l.set(j, newFidOf.getInt(l.getInt(j)-1));
			}
			l = parents.get(i);
			for (int j=0; j<l.size(); j++) {
				l.set(j, newFidOf.getInt(l.getInt(j)-1));
			}
			gidIndex.put(gids.getInt(i), i);
			sidIndex.put(sids.get(i), i);
		}

		// remember that the fids are valid
		hasConsistentFids = true;
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

		long s1 = dfreqs.getLong(fid1);
		long s2 = dfreqs.getLong(fid2);
		dfreqs.set(fid1, s2);
		dfreqs.set(fid2, s1);

		s1 = cfreqs.getLong(fid1);
		s2 = cfreqs.getLong(fid2);
		cfreqs.set(fid1, s2);
		cfreqs.set(fid2, s1);

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
        Collections.sort(fids, (fid1,fid2) -> Long.compare(dfreqOf(fid2), dfreqOf(fid1)));

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
		ensureWritable();
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
		ensureWritable();
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
		avroItem.setGid(gidOf(fid));
		avroItem.setSid(sidOfFid(fid));
		avroItem.setDfreq(dfreqOf(fid));
		avroItem.setCfreq(cfreqOf(fid));
		avroItem.setNoChildren(childrenOf(fid).size());

		// parents
		List<Integer> parentGids = avroItem.getParentGids();
		if (parentGids == null) {
			parentGids = new IntArrayList();
		} else {
			parentGids.clear();
		}
		IntList parentFids = parentsOf(fid);
		for (int i=0; i<parentFids.size(); i++) {
			int parentFid = parentFids.getInt(i);
			parentGids.add(gidOf(parentFid));
		}
		avroItem.setParentGids(parentGids);

		// properties
		List<AvroItemProperties> avroItemProperties = avroItem.getProperties();
		if (avroItemProperties == null) {
			avroItemProperties = new ArrayList<>();
		} else {
			avroItemProperties.clear();
		}
		DesqProperties properties = propertiesOf(fid);
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

	public String toJson(int fid) {
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		AvroItem avroItem = toAvroItem(fid, null);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			Encoder encoder = EncoderFactory.get().jsonEncoder(avroItem.getSchema(), out);
			itemDatumWriter.write( avroItem, encoder );
			encoder.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return out.toString();
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
			int fid = items.getInt(i);
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
		ensureWritable();
		clear();
		DatumReader<AvroItem> itemDatumReader = new SpecificDatumReader<>(AvroItem.class);
		DataFileStream<AvroItem> dataFileReader = new DataFileStream<>(in, itemDatumReader);
		AvroItem avroItem = null;
		while (dataFileReader.hasNext() && maxItems > 0) {
			maxItems--;
			avroItem = dataFileReader.next(avroItem);
			addItem(avroItem);
		}
		trim();
	}

	public void writeJson(OutputStream out) throws IOException {
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		IntList fids = topologicalSort();
		AvroItem avroItem = new AvroItem();
		Encoder encoder = EncoderFactory.get().jsonEncoder(avroItem.getSchema(), out);
		for (int i=0; i<fids.size(); i++) {
			int fid = fids.getInt(i);
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
		trim();
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
		ensureWritable();
		readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO: don't use avro here, but write manually
		byte[] bytes = toBytes();
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO: don't use avro here, but read manually
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
