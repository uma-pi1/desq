package de.uni_mannheim.desq.dictionary;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.uni_mannheim.desq.avro.AvroItem;
import de.uni_mannheim.desq.avro.AvroItemProperties;
import de.uni_mannheim.desq.io.IoUtils;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.util.CollectionUtils;
import de.uni_mannheim.desq.util.DesqProperties;
import de.uni_mannheim.desq.util.Writable2Serializer;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.spark.SparkContext;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** A set of items arranged in a hierarchy. Each item is associated with
 * <ul>
 *   <li>a stable string identifier called sid (chosen by application)</li>
 *   <li>a stable non-negative integer identifier called gid (for "global identifier").
 *       This identifier is chosen by the underlying application and does not need to fulfil any requirements (and,
 *       in particular, gids do not need to be dense).</li>
 *   <li>an internal positive numeric identifier called fid (for "frequency identifier"). The fid is not stable and
 *       may change when the dictionary is changed. Generally, fids should be dense. For many mining tasks, fids
 *       need to satisfy additional properties; see {@link #hasConsistentFids}.</li>
 *   <li>information about the item's document frequency, collection frequency, parents, and children.</li>
 *   <li>a set of properties.</li>
 * </ul>
 *
 * Instances of this class may consume substantial amount of memory if there are many items. See
 * {@link BasicDictionary} for a more light-weight variant without string identifiers.
 */
public class Dictionary extends BasicDictionary implements Externalizable, Writable {

	// -- information about items -------------------------------------------------------------------------------------

	/** Stable unique item names of this dictionary's items. Indexed by fid; null if fid not present. */
	protected final ArrayList<String> sids;

	/** Properties associated with this dictionary's items. Indexed by fid; null if fid not present. */
	protected final ArrayList<DesqProperties> properties;


	// -- indexes -----------------------------------------------------------------------------------------------------

	/** Maps sids to fids. */
	protected final Object2IntOpenHashMap<String> sidIndex;


	/** Whether this dictionary is frozen. Frozen dictionaries take less space but can't be modified. */
	protected boolean isFrozen = false;

	// -- construction ------------------------------------------------------------------------------------------------

	public Dictionary() {
		super();
		sids = new ArrayList<>();
		properties = new ArrayList<>();
		sidIndex = new Object2IntOpenHashMap<>();
		sidIndex.defaultReturnValue(-1);
	}

	/** Deep clone */
	private Dictionary(Dictionary other) {
		super(other, false, false);
		sids = new ArrayList<>(other.sids);
		sidIndex = other.sidIndex.clone();
		properties = new ArrayList<>(gids.size());
		for (int i=0; i<gids.size(); i++) {
			int gid = gids.getInt(i);
			if (gid >= 0) {
				DesqProperties p = other.properties.get(i);
				if (p != null) {
					p = new DesqProperties(p);
				}
				properties.add(p);
			} else {
				properties.add(null);
			}
		}
	}

	/** Creates a new dictionary backed by given dictionary, except parents and children. Used
	 * for {@link RestrictedDictionary}. */
	protected Dictionary(Dictionary dict, boolean dummy) {
		super(dict, false);
		sids = dict.sids;
		properties = dict.properties;
		sidIndex = dict.sidIndex;
	}

	// -- modification ------------------------------------------------------------------------------------------------

	/** Always false. Subclasses may override. */
	public boolean isReadOnly() {
		return isFrozen;
	}

	private void ensureWritable() {
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
		largestRootFid = null;
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
		largestRootFid = null;
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

	public void setDfreqOf(int fid, long dfreq) {
		ensureWritable();
		dfreqs.set(fid, dfreq);
		hasConsistentFids = null;
	}

	public void setCfreqOf(int fid, long cfreq) {
		ensureWritable();
		cfreqs.set(fid, cfreq);
	}

	/** Ensures sufficent storage storage for items */
	@Override
	public void ensureCapacity(int capacity) {
		ensureWritable();
		super.ensureCapacity(capacity);
		sids.ensureCapacity(capacity+1);
		properties.ensureCapacity(capacity+1);
		// unfortunately, fastutils doesn't allow us to specify the capacity for its hash maps
	}

	/** Reduces the memory footprint of this dictionary as much as possible. */
	@Override
	public void trim() {
		super.trim();
		int newSize = lastFid()+1;
		CollectionUtils.trim(sids, newSize);
		CollectionUtils.trim(properties, newSize);
		sidIndex.trim();
	}

	/** Freezes this dictionary. When calling this method, the dictionary is reorganized to save memory, but can't
	 * be modified anymore afterwards.
	 */
	@Override
	public void freeze() {
		if (isFrozen) return;
		super.freeze();
		// TODO: optimize properties
		isFrozen = true;
	}

	/** Returns a copy of this dictionary. Even if this dictionary is frozen, the copy will be writable. */
	@Override
	public Dictionary deepCopy() {
		return new Dictionary(this);
	}

	// -- querying ----------------------------------------------------------------------------------------------------

	public boolean containsSid(String sid) {
		return sidIndex.containsKey(sid);
	}

	/** Returns the fid for the specified sid or -1 if not present */
	public int fidOf(String sid) {
		return sidIndex.getInt(sid);
	}

	/** Returns the sid for the specified fid or null if not present */
	public String sidOfFid(int fid) {
		return fid<sids.size() ? sids.get(fid) : null;
	}

	/** Returns the gid for the specified sid or -1 if not present */
	public int gidOf(String sid) {
		int fid = fidOf(sid);
		return fid < 0 ? fid : gidOf(fid);
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

	/** Returns all sids. */
	public Set<String> sids() {
		return sidIndex.keySet();
	}

	/** Returns the sid for the specified gid or null if not present */
	public String sidOfGid(int gid) {
		int fid = fidOf(gid);
		return fid < 0 ? null : sidOfFid(fid);
	}

	/** Returns the properties of the specified fid (can be null) or null if not present */
	public DesqProperties propertiesOf(int fid) {
		return properties.get(fid);
	}

	/** Returns a {@link BasicDictionary} backed by this dictionary. The returned copy does not hold any references
	 * to the sids and properties of this dictionary. */
	public BasicDictionary shallowCopyAsBasicDictionary() {
		return new BasicDictionary(this, true);
	}

	/** Returns a memory-optimized {@link BasicDictionary} copy of this dictionary. When the dictionary is frozen,
	 * this method has the same behaviour as {@link #shallowCopyAsBasicDictionary()}. */
	public BasicDictionary deepCopyAsBasicDictionary() {
		return isFrozen ? shallowCopyAsBasicDictionary() : super.deepCopy();
	}

	// -- computing supports ------------------------------------------------------------------------------------------
	
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
		largestRootFid = null;
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

		IntList l1 = parents.get(fid1);
		IntList l2 = parents.get(fid2);
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


	// -- I/O ---------------------------------------------------------------------------------------------------------

	public static Dictionary loadFrom(String fileName) throws IOException {
		if (fileName.startsWith("hdfs")) {
			throw new IOException("Can't read from HDFS. Use method loadFrom(String fileName, SparkContext sc) " +
					"instead.");
		}
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

	/** Reads file from any Hadoop-supported file system. */
	public static Dictionary loadFrom(String fileName, SparkContext sc) throws IOException {
		Dictionary dict = new Dictionary();
		dict.read(fileName, IoUtils.readHadoop(fileName, sc));
		return dict;
	}

	/** Reads a dictionary from a file. Automatically determines the right format based on file extension. */
	public void read(File file) throws IOException {
		read(file.getName(), new FileInputStream(file));
	}

	/** Reads a dictionary from an URL. Automatically determines the right format based on file extension. */
	public void read(URL url) throws IOException {
		read(url.getFile(), url.openStream());
	}

	/** Reads a dictionary from an input stream. The filename is only used to determine the right format (based
	 * on the file extension). */
	private void read(String fileName, InputStream inputStream) throws IOException {
		ensureWritable();
		if (fileName.endsWith(".json")) {
			readJson(inputStream);
			return;
		}
		if (fileName.endsWith(".json.gz")) {
			readJson(new GZIPInputStream(inputStream));
			return;
		}
		if (fileName.endsWith(".avro")) {
			readAvro(inputStream);
			return;
		}
		if (fileName.endsWith(".avro.gz")) {
			readAvro(new GZIPInputStream(inputStream));
			return;
		}
		throw new IllegalArgumentException("unknown file extension: " + fileName);
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

		// each item
		DesqProperties EMPTY_PROPERTIES = new DesqProperties();
		IntList fids = topologicalSort();
		for (int i=0; i<fids.size(); i++) {
			int fid = fids.getInt(i);
			WritableUtils.writeVInt(out, fid);
			WritableUtils.writeVInt(out, gids.getInt(fid));
			out.writeUTF(sids.get(fid));
			WritableUtils.writeVLong(out, dfreqs.getLong(fid));
			WritableUtils.writeVLong(out, cfreqs.getLong(fid));

			DesqProperties properties = this.properties.get(fid);
			if (properties == null) {
				EMPTY_PROPERTIES.write(out);
			} else {
				properties.write(out);
			}

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

		// each item
		DesqProperties propertiesBuffer = new DesqProperties();
		for (int i=0; i<size; i++) {
			int fid = WritableUtils.readVInt(in);
			int gid = WritableUtils.readVInt(in);
			String sid = in.readUTF();
			long dfreq = WritableUtils.readVLong(in);
			long cfreq = WritableUtils.readVLong(in);

			propertiesBuffer.readFields(in);
			DesqProperties properties = null;
			if (propertiesBuffer.size() > 0) {
				properties = propertiesBuffer;
				propertiesBuffer = new DesqProperties();
			}

			int noParents = WritableUtils.readVInt(in);
			IntArrayList parents = new IntArrayList(noParents);
			int noChildren = WritableUtils.readVInt(in);
			IntArrayList children = new IntArrayList(noChildren);

			addItem(fid, gid, sid, dfreq, cfreq, parents, children, properties);

			for (int j=0; j<noParents; j++) {
				addParent(fid, WritableUtils.readVInt(in));
			}
		}

		assert this.size == size;
		this.isForest = isForest;
		this.hasConsistentFids = hasConsistentFids;
	}

	public static final class KryoSerializer extends Writable2Serializer<Dictionary> {
		@Override
		public Dictionary newInstance(Kryo kryo, Input input, Class<Dictionary> type) {
			return new Dictionary();
		}
	}
}
