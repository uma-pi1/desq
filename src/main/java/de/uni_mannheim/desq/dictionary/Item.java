package de.uni_mannheim.desq.dictionary;

import de.uni_mannheim.desq.avro.AvroItem;
import de.uni_mannheim.desq.avro.AvroItemProperties;
import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;
import java.util.*;

/** A single item in a dictionary.  */
public final class Item {
	/** Stable global identifier of this item */
	public final int gid;

	/** Unique name of this item */
	public final String sid;

	/** Internal "frequency" identifier of this item used to support efficient de.uni_mannheim.desq.old.mining. The identifier is not
	 * necessarily stable. Generally, the frequency identifier needs to satisfy certain properties, which depend
	 * on the actual miner being used. In most cases, these properties match the ones described in
	 * {@link Dictionary#recomputeFids()}}. */
	public int fid = -1;

	/** Collection frequency of this item and its ascendants */
	public int cFreq = 0;

    /** Document frequency of this item and its ascendants */
    public int dFreq = 0;

    /** Children of this item */
	public final List<Item> children = new ArrayList<>();

    /** Parents of this item. */
    public final List<Item> parents = new ArrayList<>();

    /** Other properties associated with this item */
    public DesqProperties properties = new DesqProperties();
	
	public Item(int gid, String sid) {
		this.gid = gid;
		this.sid = sid;
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

	/** Returns a comparator that compares by {@link de.uni_mannheim.desq.dictionary.Item#dFreq} descending,
	 * then by gid ascending. */
	public static Comparator<Item> dfreqDecrComparator() {
		return (o1, o2) -> {
            int freqDif = o2.dFreq - o1.dFreq;
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
		Iterator<String> keysIt = properties.getKeys();
		while (keysIt.hasNext()) {
			String key = keysIt.next();
			String value = properties.getString(key, null);
			avroItemProperties.add(new AvroItemProperties(key, value));
		}
		avroItem.setProperties(avroItemProperties);

		return avroItem;
	}

	public static Item fromAvroItem(AvroItem avroItem, Dictionary dict) {
		// basic fields
		Item item = new Item(avroItem.getGid(), avroItem.getSid().toString());
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
		for (AvroItemProperties property : avroItem.getProperties()) {
			String key = property.getKey().toString();
			String value = property.getValue() != null ? property.getValue().toString() : null;
			item.properties.setProperty(key, value);
		}

		return item;
	}
}
