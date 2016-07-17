package de.uni_mannheim.desq.dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.utils.IntSetUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

/** A set of items arranged in a hierarchy */ 
public class Dictionary {
	// indexes
	Map<Integer, Item> itemsById = new HashMap<Integer, Item>();
	Map<Integer, Item> itemsByFid = new HashMap<Integer, Item>();
	Map<String, Item> itemsBySid = new HashMap<String, Item>();
	
	// -- updating the hierarchy ------------------------------------------------------------------
	
	/** Adds a new item to the hierarchy. If the fid equals 0, it won't be indexed */
	public void addItem(Item item) {
		if (containsId(item.id)) {
			throw new IllegalArgumentException("Item id '" + item.id + "' exists already");
		}
		if (itemsBySid.containsKey(item.sid)) {
			throw new IllegalArgumentException("Item sid '" + item.sid + "' exists already");
		}
		if (item.fid >= 0 && itemsByFid.containsKey(item.fid)) {
			throw new IllegalArgumentException("Item fid '" + item.id + "' exists already");
		}
		itemsById.put(item.id, item);
		if (item.fid >= 0) itemsByFid.put(item.fid, item);
		itemsBySid.put(item.sid, item);		
	}
	
	/** Updates the counts of the hierarchy by adding the given input sequence. 
	 * The two insets are used for temporary storage. */
	private void incCounts(IntList inputSequence, IntSet seenItems, IntSet ancItems, boolean fid) {
		seenItems.clear();
		for(int i=0; i<inputSequence.size(); i++) {
			int ii = inputSequence.getInt(i);
			Item item = fid ? getItemByFid(ii) : getItemById(ii);
			ancItems.clear();
			ancItems.add(item.id);
			addAscendantIds(item, ancItems);
			for (int id : ancItems) {
				getItemById(id).cFreq++;
			}
			seenItems.addAll(ancItems);
		}
		for (int id : seenItems) {
			getItemById(id).dFreq++;
		}
	}
	
	/** Updates the counts of the hierarchy by adding the given input sequences. Does
	 * not modify fids, so those may be inconsistent afterwards. 
	 * 
	 * TODO: optimize by deferring updates to collection frequency to the end
	 */
	public void incCounts(SequenceReader reader) throws IOException {
		IntList inputSequence = new IntArrayList();
		IntSet seenItems = new IntOpenHashSet();
		IntSet ancItems = new IntOpenHashSet();
		while (reader.read(inputSequence)) {
			incCounts(inputSequence, seenItems, ancItems, reader.usesFids());
		}
	}
	
	/** Sets fid of all items to -1 */
	public void clearFids() {
		itemsByFid.clear();
		for (Item item : itemsById.values()) {
			item.fid = -1;
		}
	}

	/** Sets cFreq and dFreq counts of all items to 0 */
	public void clearCounts() {
		for (Item item : itemsById.values()) {
			item.cFreq = 0;
			item.dFreq = 0;
		}
	}
	
	// -- access to indexes -----------------------------------------------------------------------
	
	public Collection<Item> allItems() {
		return itemsById.values();
	}
	
	/** Checks whether there is an item with the given ID in the hierarchy. */
	public boolean containsId(int itemId) {
		return itemsById.containsKey(itemId);
	}
	
	/** Returns the item with the given id (or null if no such item exists) */
	public Item getItemById(int itemId) {
		return itemsById.get(itemId);
	}
	/** Returns all items for the given fids */
	public List<Item> getItemsByIds(IntCollection itemIds) {
		List<Item> items = new ArrayList<Item>();
		getItemsByIds(itemIds, items);
		return items;
	}

	/** Stores all items for the given fids in the target list */
	public void getItemsByIds(IntCollection itemFids, List<Item> target) {
		target.clear();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			target.add(getItemById(it.nextInt()));
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
		List<Item> items = new ArrayList<Item>();
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
			if (!descendants.contains(itemFid)) {
				descendants.add(itemFid);
				addDescendantFids(getItemByFid(itemFid), descendants);
			}
		}
		return IntSetUtils.optimize(descendants);
	}
	
	/** Adds all descendants of the specified item to itemFids, excluding the given item and all 
	 * descendants of items already present in itemFids. */	
	public void addDescendantFids(Item item, IntSet itemFids) {
		for (Item child : item.children) {
			if (!itemFids.contains(child.fid)) {
				itemFids.add(child.fid);
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
			if (!ascendants.contains(itemFid)) {
				ascendants.add(itemFid);
				addAscendantFids(getItemByFid(itemFid), ascendants);
			}
		}
		return ascendants;
	}
	
	/** Adds all ascendants of the specified item to itemFids, excluding the given item and all 
	 * ascendants of items already present in itemFids. */	
	public void addAscendantFids(Item item, IntSet itemFids) {
		for (Item parent : item.parents) {
			if (!itemFids.contains(parent.fid)) {
				itemFids.add(parent.fid);
				addAscendantFids(getItemByFid(parent.fid), itemFids);
			}
		}
	}
	
	/** Adds all ascendants of the specified item to itemFids, excluding the given item and all 
	 * ascendants of items already present in itemFids. */	
	public void addAscendantIds(Item item, IntSet itemIds) {
		for (Item parent : item.parents) {
			if (!itemIds.contains(parent.id)) {
				itemIds.add(parent.id);
				addAscendantIds(getItemById(parent.id), itemIds);
			}
		}
	}

	/** Returns a copy of this dictionary that contains only the specified items (including
	 * the *direct* links between these items).
	 * 
	 * TODO: also add indirect links? (takes some thought to figure out which links to acutally add and how to do this 
	 * reasonably efficiently; perhaps helpful: a method that removes "unnecessary" links)
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
		
		return dict;
	}
	
	// -- utility methods -------------------------------------------------------------------------
	
	/** Returns array a where a[i] is document frequency of item with fid i. Be careful when fids
	 * are sparse; the resulting array might then get big */
	public IntList getFlist() {
		IntList flist = new IntArrayList();
		flist.size(itemsByFid.size()+1);
		for(Entry<Integer, Item> entry : itemsByFid.entrySet()) {
			int fid = entry.getKey().intValue();
			if (fid>flist.size()) flist.size(fid+1);
			flist.set(fid, entry.getValue().dFreq);
		}
		return flist;
	}
	
	public void idsToFids(IntList ids) {
		for (int i=0; i<ids.size(); i++) {
			int id = ids.getInt(i);
			int fid = getItemById(id).fid;
			ids.set(i, fid);	
		}
	}
	
	public void fidsToIds(IntList fids) {
		for (int i=0; i<fids.size(); i++) {
			int fid = fids.getInt(i);
			int id = getItemByFid(fid).id;
			fids.set(i, id);	
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
			Item item = getItemById(id);
			item.fid = fid;
			itemsByFid.put(fid, item);
		}
	}
	
	/** Performs a topological sort of the items in this dictionary, respecting document 
	 * frequencies. Throws an IllegalArgumentException if there is a cycle.
	 *  
     * @author Keith Schwarz (htiek@cs.stanford.edu) 
     */
    private IntList topologicalSort() {
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
        List<Item> items = new ArrayList<Item>(allItems());
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
     * @param g The graph in which to perform the search.
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
        if (visitedIds.contains(item.id)) {
            /* There are two cases to consider.  First, if this node has
             * already been expanded, then it's already been assigned a
             * position in the final topological sort and we don't need to
             * explore it again.  However, if it hasn't been expanded, it means
             * that we've just found a node that is currently being explored,
             * and therefore is part of a cycle.  In that case, we should 
             * report an error.
             */
            if (expandedIds.contains(item.id)) return;
            throw new IllegalArgumentException("Graph contains a cycle.");
        }
        
        /* Mark that we've been here */
        visitedIds.add(item.id);

        /* Recursively explore all of the node's predecessors. */
        for (Item predecessor: item.parents)
            explore(predecessor, orderingIds, visitedIds, expandedIds);

        /* Having explored all of the node's predecessors, we can now add this
         * node to the sorted ordering.
         */
        orderingIds.add(item.id);

        /* Similarly, mark that this node is done being expanded. */
        expandedIds.add(item.id);
    }
}
