package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.avro.AvroItem;
import de.uni_mannheim.desq.avro.AvroItemProperties;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.spark.ItemWithKnownSizeEstimation;
import org.apache.spark.util.SizeEstimator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/** A single item in a dictionary.  */
public final class Item extends ItemWithKnownSizeEstimation {
	/** Stable global identifier of this item */
	public final int gid;

	/** Unique name of this item */
	public final String sid;

	/** Internal "frequency" identifier of this item used to support efficient mining. The identifier is not
	 * necessarily stable. Generally, the frequency identifier needs to satisfy certain properties, which depend
	 * on the actual miner being used. In most cases, these properties match the ones described in
	 * {@link Dictionary#recomputeFids()}}. */
	public int fid = -1;

	/** Collection frequency of this item and its ascendants */
	public long cFreq = 0;

    /** Document frequency of this item and its ascendants */
    public long dFreq = 0;

    /** Children of this item */
	public final List<Item> children;

    /** Parents of this item. */
    public final List<Item> parents;

    /** Other properties associated with this item. Can be null. */
    public DesqProperties properties = null; // null default to save memory
	
	public Item(int gid, String sid) {
		this.gid = gid;
		this.sid = sid;
		this.parents = new ArrayList<>(1);
		this.children = new ArrayList<>(0);
	}

	private Item(int gid, String sid, int noParents, int noChildren) {
		this.gid = gid;
		this.sid = sid;
		this.parents = new ArrayList<>(noParents);
		this.children = new ArrayList<>(noChildren);
	}

	/** Connects child and parent. Modifies child.parents and parent.children. */
	public static void addParent(Item child, Item parent) {
		if (child == null || parent == null) throw new IllegalArgumentException();
		child.parents.add(parent);
		parent.children.add(child);
	}
	
	public String toString() {
		return sid;
	}

	public String toJson() {
		DatumWriter<AvroItem> itemDatumWriter = new SpecificDatumWriter<>(AvroItem.class);
		AvroItem avroItem = new AvroItem();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			Encoder encoder = EncoderFactory.get().jsonEncoder(avroItem.getSchema(), out);
			itemDatumWriter.write( toAvroItem(null), encoder );
			encoder.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return out.toString();
	}

	/** Returns a copy of this item but does not copy childs and parents and shares the properties */
	public Item shallowCopyWithoutEdges() {
		Item item = new Item(gid, sid);
		item.fid = fid;
		item.cFreq = cFreq;
		item.dFreq = dFreq;
		item.properties = properties;
		return item;
	}

	/** Returns a comparator that compares by {@link Item#dFreq} descending,
	 * then by gid ascending. */
	public static Comparator<Item> dfreqDecrComparator() {
		return (o1, o2) -> {
            int freqDif = Long.compare(o2.dFreq, o1.dFreq);
            if (freqDif != 0) return freqDif;
            return o1.gid - o2.gid;
        };

	}

	public AvroItem toAvroItem(AvroItem avroItem) {
		if (avroItem == null) {
			avroItem = new AvroItem();
		}

		// basic fields
		avroItem.setGid(gid);
		avroItem.setSid(sid);
		avroItem.setFid(fid);
		avroItem.setCFreq(cFreq);
		avroItem.setDFreq(dFreq);
		avroItem.setNoChildren(children.size());

		// parents
		List<Integer> parentGids = avroItem.getParentGids();
		if (parentGids == null) {
			parentGids = new IntArrayList();
 		} else {
			parentGids.clear();
		}
		for (Item parentItem : parents) {
			parentGids.add(parentItem.gid);
		}
		avroItem.setParentGids(parentGids);

		// properties
		List<AvroItemProperties> avroItemProperties = avroItem.getProperties();
		if (avroItemProperties == null) {
			avroItemProperties = new ArrayList<>();
		} else {
			avroItemProperties.clear();
		}
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

	public static Item fromAvroItem(AvroItem avroItem, Dictionary dict) {
		// basic fields
		int noParents = avroItem.getParentGids().size();
		int noChildren = avroItem.getNoChildren();
		Item item = new Item(avroItem.getGid(), avroItem.getSid(), noParents, noChildren);
		item.fid = avroItem.getFid();
		item.cFreq = avroItem.getCFreq();
		item.dFreq = avroItem.getDFreq();

		// parents
		for (Integer parentGid : avroItem.getParentGids()) {
			Item parentItem = dict.getItemByGid(parentGid);
			if (parentItem == null) {
				throw new RuntimeException("parent item with gid=" + parentGid + " not present");
			}
			addParent(item, parentItem);
		}

		// properties
		if (avroItem.getProperties().size()>0) {
			item.properties = new DesqProperties(avroItem.getProperties().size());
			for (AvroItemProperties property : avroItem.getProperties()) {
				String key = property.getKey();
				String value = property.getValue() != null ? property.getValue() : null;
				item.properties.setProperty(key, value);
			}
		}

		return item;
	}

	/** Hack to make Spark estimate the in-memory size of a Dictionary correctly. This code does not count the
	 * in-memory size of the items in the children and parents lists, but instead only counts the corresponding
	 * pointer sizes. This still gives a decent estimate of dictinary size because every item (being referenced)
	 * is stored in the dictionary anyway and thus will be counted. */
	@Override
	public long estimatedSize() {
		ProxyItem proxyItem = new ProxyItem();
		proxyItem.gid = gid;
		proxyItem.sid = sid;
		proxyItem.fid = fid;
		proxyItem.cFreq = cFreq;
		proxyItem.dFreq = dFreq;
		proxyItem.properties = properties;

		proxyItem.children = new ArrayList<>(children.size());
		for (int i=0; i<children.size(); i++)
			proxyItem.children.add(null);
		proxyItem.parents = new ArrayList<>(parents.size());
		for (int i=0; i<parents.size(); i++)
			proxyItem.parents.add(null);

		return SizeEstimator.estimate(proxyItem);
	}

	private static class ProxyItem {
		int gid;
		String sid;
		int fid;
		long cFreq;
		long dFreq;
		List<Object> children;
		List<Object> parents;
		DesqProperties properties;
	}
}
