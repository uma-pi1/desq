package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.commons.io.FilenameUtils;

public class IndexPatriciaTrie {

    private int rootId;  //index of root
    private int currentNodeId; //index of new node
    private LongList nodeSupport;
    private Int2ObjectMap<IntList> nodeChildren;
    private Int2ObjectMap<IntList> nodeItems;
    private BooleanList nodeIsFinal;
    private BooleanList nodeIsLeaf;
    //during construction only:
    private Int2ObjectMap<Int2IntMap> nodeChildrenByFirstItem;

    private Dictionary dict;

    public IndexPatriciaTrie(Dictionary dict) {

        this.currentNodeId = -1;

        this.nodeSupport = new LongArrayList();
        this.nodeChildren = new Int2ObjectArrayMap<>();
        this.nodeItems = new Int2ObjectArrayMap<>();
        this.nodeIsFinal = new BooleanArrayList();
        this.nodeIsLeaf = new BooleanArrayList();
        this.nodeChildrenByFirstItem = new Int2ObjectArrayMap<>();

        //init root node with empty list
        this.rootId = newNode(new IntArrayList(), (long) 0, false, true);

        if (dict != null) {
            this.dict = dict;
        }
    }

    private int newNode(IntList items, long support, boolean isFinal, boolean isLeaf) {
        //Next Node Id
        int idx = ++currentNodeId;

        //Fill values with corresponding node index
        this.nodeItems.put(idx, items);
        this.nodeSupport.add(idx, support);
        this.nodeIsFinal.add(idx, isFinal);
        this.nodeIsLeaf.add(idx, isLeaf);
        //Init without children
        this.nodeChildren.put(idx, new IntArrayList());
        this.nodeChildrenByFirstItem.put(idx, new Int2IntOpenHashMap());

        return idx;
    }

    //getter
    public int getRootId() {
        return rootId;
    }

    public long getSupport(int nodeId) {
        return nodeSupport.getLong(nodeId);
    }

    public boolean isFinal(int nodeId) {
        return nodeIsFinal.getBoolean(nodeId);
    }

    public boolean isLeaf(int nodeId) {
        return nodeIsLeaf.getBoolean(nodeId);
    }

    public IntList getItems(int nodeId) {
        return nodeItems.get(nodeId);
    }

    public int getItem(int nodeId, int itemIdx) {
        return getItems(nodeId).getInt(itemIdx);
    }

    public IntList getChildren(int nodeId) {
        return nodeChildren.get(nodeId);
    }

    public int getChildrenStartingWith(int nodeId, int item) {
        return nodeChildrenByFirstItem.get(nodeId).getOrDefault(item, -1);
    }

    //setter

    public boolean setFinal(int nodeId, boolean isFinal) {
        return this.nodeIsFinal.set(nodeId, isFinal);
    }


    public void addChild(int parentId, int childId, int firstItem) {
        nodeChildren.get(parentId).add(childId);
        nodeChildrenByFirstItem.get(parentId).put(firstItem, childId);
        nodeIsLeaf.set(parentId, false);
    }

    public void addChild(int parentId, int childId) {
        addChild(parentId, childId, nodeItems.get(childId).getInt(0));
    }

    public void moveChildren(int sourceId, int targetId) {
        //Copy
        nodeChildrenByFirstItem.get(targetId).putAll(nodeChildrenByFirstItem.get(sourceId));
        if(nodeChildren.get(targetId).addAll(nodeChildren.get(sourceId)))
            nodeIsLeaf.set(targetId, false);
        //Remove
        nodeChildrenByFirstItem.get(sourceId).clear();
        nodeChildren.get(sourceId).clear();
    }

    public void incSupport(int nodeId, long support) {
        nodeSupport.set(nodeId, nodeSupport.getLong(nodeId) + support);
    }

    public String getNodeString(int nodeId) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        //builder.append(id);
        builder.append("[");
        for (String s : dict.sidsOfFids(getItems(nodeId))) {
            if (!first) builder.append(",");
            builder.append(s);
            if (first) first = false;
        }
        builder.append("]@");
        builder.append(getSupport(nodeId));
        return builder.toString();
    }


    public int size() {
        return currentNodeId + 1;
    }

    public void clear() {
        this.currentNodeId = -1;

        this.nodeSupport.clear();
        this.nodeChildren.clear();
        this.nodeItems.clear();
        this.nodeIsFinal.clear();
        this.nodeIsLeaf.clear();
        this.nodeChildrenByFirstItem.clear();

        //init root node with empty list
        this.rootId = newNode(new IntArrayList(), (long) 0, false, true);
    }

    public void addItems(IntList fids, long support) {
        addItems(rootId, fids, support);
    }

    /**
     * Adding fids into the trie by traversing (starting at given start node)
     *
     * @param startNodeId node of trie to start at
     * @param items       to be added
     * @param support     support/weight of these items
     */

    public void addItems(int startNodeId, IntList items, long support) {
        //traverse trie and inc support till nodes do not match or anymore or end of list
        IntListIterator inputIt = items.iterator();
        int currentNodeId = startNodeId;
        IntListIterator currentNodeIt = getItems(currentNodeId).iterator();

        while (inputIt.hasNext()) { //iterate over input items
            int currentInputItem = inputIt.next();
            // Compare input with existing items in node
            if (currentNodeIt.hasNext()) { //and iterate in parallel over node items
                //compare next item in current node
                int nodeItem = currentNodeIt.next();
                //Case: item list differs -> split in common prefix and extend new sequence part
                if (currentInputItem != nodeItem) {
                    //node item and input item differ -> split node and extend with remaining!
                    splitNode(currentNodeId, nodeItem);
                    setFinal(currentNodeId, false); //is only sub-sequence
                    //and add new node with remaining input items
                    expandTrie(currentNodeId, createIntList(currentInputItem, inputIt), support, true);
                    break; //remaining input added -> finished processing
                } //else: same item in input and in node so far

                //Case: item list is shorter than node Items -> split and update the first part (current node)
                else if (!inputIt.hasNext() && currentNodeIt.hasNext()) {
                    splitNode(currentNodeId, currentNodeIt.next());
                    //stays final because sequence ends here
                    break;
                }
                //Case: sequence fits in existing trie
                else if (!inputIt.hasNext() && !currentNodeIt.hasNext()) {
                    //ensure that this node is marked as final, because sequence ends here
                    setFinal(currentNodeId, true);
                    break;
                }
                //Case: more input items than items in node -> children string with item or expand
            } else {
                //try to get child node starting with item
                int nextNodeId = getChildrenStartingWith(currentNodeId, currentInputItem);
                if (nextNodeId == -1) {
                    //no next node starting with input item -> expand with new node containing all remaining
                    expandTrie(currentNodeId, createIntList(currentInputItem, inputIt), support, true);
                    break; //remaining input added -> finished processing
                } else {
                    //found child node starting with current item
                    //go to next node, but inc support and clean up current
                    incSupport(currentNodeId, support);

                    currentNodeId = nextNodeId;
                    //skip the first item (already checked via hash key)
                    currentNodeIt = getItems(currentNodeId).iterator();
                    currentNodeIt.next();
                    //continue;
                }
            }
        }
        //inc support of last visited node and clean up
        //doing it after loop ensures that the last node is considered if there was no expand
        incSupport(currentNodeId, support);
    }

    //Construct the remaining items (TODO: more efficient way?)
    private IntList createIntList(int firstItem, IntListIterator remainingItems) {
        IntList items = new IntArrayList();
        items.add(firstItem);
        if (remainingItems.hasNext()) { //there are more items to add
            remainingItems.forEachRemaining(items::add);
        }
        return items;
    }

    private int expandTrie(int startNode, IntList items, long support, boolean isFinal) {
        //Create new node
        int newNode = newNode(items, support, isFinal, true);
        //Set pointer in parent node
        addChild(startNode, newNode, items.getInt(0));
        return newNode;
    }

    /**
     * Split node at given separator.
     * Splits an existing node by altering the existing such that it keeps items
     * and parents do not need to be adjusted. The remaining Items are moved to a new node
     * which is attached as child node to the existing
     *
     * @param nodeId        to be split
     * @param separatorItem first item of new (second) node
     * @return the new node
     */
    private int splitNode(int nodeId, int separatorItem) {
        //separateItems
        IntList items = this.nodeItems.get(nodeId);
        int idx = items.indexOf(separatorItem);
        //determine to be removed items and copy them
        IntList removed = new IntArrayList(items.subList(idx, items.size()));
        //delete remaining based on idx
        items.removeElements(idx, items.size());

        //Create new node
        //if original node is not final, child is neither
        //if original node stays final is decided in addItem node (depends on case)
        int newNode = newNode(removed, getSupport(nodeId), isFinal(nodeId), true);
        //Move children to new node
        moveChildren(nodeId, newNode);
        //Add new node as child
        addChild(nodeId, newNode, removed.getInt(0));
        return newNode;
    }

    /**
     * Exports the trie using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...)
     */
    public void exportGraphViz(String file, int maxDepth) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        expandGraphViz(rootId, automatonVisualizer, maxDepth);
        //automatonVisualizer.endGraph(trie.getRoot().toString());
        automatonVisualizer.endGraph();
    }

    private void expandGraphViz(int nodeId, AutomatonVisualizer viz, int depth) {
        int nextDepth = depth - 1;
        for (int childId : getChildren(nodeId)) {
            //viz.add(node.toString(), String.valueOf(child.firstItem()), child.toString());
            viz.add(String.valueOf(nodeId), getNodeString(childId), String.valueOf(childId));
            if (isFinal(childId)) {
                //viz.addFinalState(child.toString());
                viz.addFinalState(String.valueOf(childId), isLeaf(childId));
            }
            if (!isLeaf(childId) && depth > 0) {
                expandGraphViz(childId, viz, nextDepth);
            }
        }
    }
}