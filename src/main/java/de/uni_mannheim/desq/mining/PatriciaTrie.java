package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.commons.io.FilenameUtils;

import java.util.*;

public class PatriciaTrie {

    private TrieNode root;
    private int currentNodeId;

    private List<TrieNode> nodes;

    public PatriciaTrie() {
        //init root node with empty list
        this.nodes = new ArrayList<>();
        this.currentNodeId = -1;
        this.root = new TrieNode(new IntArrayList(), (long) 0,false, true, ++currentNodeId);
        nodes.add(currentNodeId, root);
    }


    public TrieNode getRoot(){
        return root;
    }

    public TrieNode getNodeById(int id){
        return nodes.get(id);
    }

    public int size(){
        return currentNodeId + 1;
    }

    public void clear(){
        nodes.clear();
        currentNodeId = -1;
        root.removeAllChildren();
        root = new TrieNode(new IntArrayList(), (long) 0,false, true, ++currentNodeId);
        nodes.add(currentNodeId, root);
    }

    public void addItems(IntList fids) {
        addItems(fids, (long) 1);
    }

    public void addItems(IntList fids, long support) {
        addItems(root, fids, support,null);
    }

    /**
     * Adding fids into the trie by traversing (starting at given start node)
     * @param startNode node of trie to start at
     * @param items to be added
     * @param support support/weight of these items
     */

    public void addItems(TrieNode startNode, IntList items, long support, Producer producer) {
        //traverse trie and inc support till nodes do not match or anymore or end of list
        IntListIterator it = items.iterator();
        TrieNode currentNode = startNode;

        while (it.hasNext()) { //iterate over input items
            int currentItem = it.next();
            // Compare input with existing items in node
            if (currentNode.itemIterator().hasNext()) { //and iterate in parallel over node items
                //compare next item in current node
                int nodeItem = currentNode.itemIterator().next();
                //Case: item list differs -> split in common prefix and extend new sequence part
                if (currentItem != nodeItem) {
                    //node item and input item differ -> split node and extend with remaining!
                    splitNode(currentNode, nodeItem);
                    currentNode.setFinal(false); //is only sub-sequence
                    //and add new node with remaining input items
                    expandTrie(currentNode, createIntList(currentItem, it),support, producer,true);
                    break; //remaining input added -> finished processing
                } //else: same item in input and in node so far

                //Case: item list is shorter than node Items -> split and update the first part (current node)
                else if(!it.hasNext() && currentNode.itemIterator().hasNext()) {
                    splitNode(currentNode, currentNode.itemIterator().next());
                    //stays final because sequence ends here
                    break;
                }
                //Case: sequence fits in existing trie
                else if(!it.hasNext() && !currentNode.itemIterator().hasNext()){
                    //ensure that this node is marked as final, because sequence ends here
                    currentNode.setFinal(true);
                    break;
                }
            //Case: more input items than items in node -> children string with item or expand
            } else {
                //try to get child node starting with item
                TrieNode nextNode = currentNode.getChildrenStartingWith(currentItem);
                if (nextNode == null) {
                    //no next node starting with input item -> expand with new node containing all remaining
                    expandTrie(currentNode, createIntList(currentItem, it), support, producer,true);
                    break; //remaining input added -> finished processing
                } else {
                    //found child node starting with current item
                    //go to next node, but inc support and clean up current
                    currentNode.incSupport(support);
                    currentNode.clearIterator();
                    currentNode = nextNode;
                    //skip the first item (already checked via hash key)
                    currentNode.itemIterator().next();
                    //continue;
                }
            }
        }
        //inc support of last visited node and clean up
        //doing it after loop ensures that the last node is considered if there was no expand
        currentNode.incSupport(support);
        currentNode.addProducer(producer);
        currentNode.clearIterator();
    }
    // Add a single item after a given node (used for pattern growth)
    public void appendItem(TrieNode startNode, int item, long support, Producer producer, boolean isFinal) {
        //IntList is reused in node
        IntList items = new IntArrayList();
        items.add(item);
        TrieNode nextNode = startNode.getChildrenStartingWith(item);
        if (nextNode == null) {
            //no next node starting with input item -> expand with new node containing the item
            expandTrie(startNode, items, support, producer, isFinal);
        } else {
            //found child node starting with current item
            //Check if the node contains more items -> split
            if(nextNode.items.size() > 1) {
                //but it has more items -> split after first item
                splitNode(nextNode, nextNode.items.get(1));
            }
            //update the node containing the item
            if(isFinal) nextNode.setFinal(isFinal); //only ensure true, but do not override it with false
            //Check if it was already produced by same node -> skip (avoiding duplicates)
            for(Producer p: nextNode.producers){
                if(producer.node == p.node) return;
            }
            nextNode.incSupport(support);
            nextNode.addProducer(producer);
        }
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

    private TrieNode expandTrie(TrieNode startNode, IntList items, long support, Producer producer, boolean isFinal) {
        //Create new node
        TrieNode newNode = new TrieNode(items, support,isFinal,true, ++currentNodeId);
        //Set producer if provided
        newNode.addProducer(producer);
        //Set pointer in parent node
        startNode.addChild(newNode);
        //add new node to list
        nodes.add(currentNodeId, newNode);
        return newNode;
    }

    /**
     * Split node at given separator.
     * Splits an existing node by altering the existing such that it keeps items
     * and parents do not need to be adjusted. The remaining Items are moved to a new node
     * which is attached as child node to the existing
     * @param node to be split
     * @param separatorItem first item of new (second) node
     * @return the new node
     */
    private TrieNode splitNode(TrieNode node, int separatorItem) {
        IntList remaining = node.separateItems(separatorItem);
        //remove children pointer from existing node
        HashMap<Integer, TrieNode> existingChildren = node.removeAllChildren();
        //expand from existing node with remaining items
        //if original node is not final, child is neither
        //if original node stays final is decided in addItem node (depends on case)
        TrieNode newNode = expandTrie(node, remaining, node.support, null, node.isFinal);
        //copy list, it might change independently afterwards
        newNode.producers.addAll(node.producers);
        //remove the producer list (they only produced the sequence ending at new node)
        node.producers.clear();
        //add existing children to new node
        if (!existingChildren.isEmpty()) {
            newNode.children = existingChildren;
            newNode.isLeaf = false;
        }
        return newNode;
    }

    /**
     * Exports the trie using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...)
     */
    public void exportGraphViz(String file, Dictionary dict, int maxDepth) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        expandGraphViz(root, automatonVisualizer, dict,maxDepth);
        //automatonVisualizer.endGraph(trie.getRoot().toString());
        automatonVisualizer.endGraph();
    }

    private void expandGraphViz(TrieNode node, AutomatonVisualizer viz, Dictionary dict, int depth) {
        int nextDepth = depth - 1;
        for (TrieNode child : node.collectChildren()) {
            //viz.add(node.toString(), String.valueOf(child.firstItem()), child.toString());
            viz.add(String.valueOf(node.id), child.toString(dict), String.valueOf(child.id));
            if (child.isFinal) {
                //viz.addFinalState(child.toString());
                viz.addFinalState(String.valueOf(child.id),child.isLeaf);
            }
            if (!child.isLeaf && depth > 0) {
                expandGraphViz(child, viz, dict, nextDepth);
            }
        }
    }



    // ------------------------- TRIE NODE -----------------------------

    public class TrieNode {
        // unique id
        private int id;
        //The sequence of FIDs
        protected IntList items;
        //An iterator (singleton) over the fids
        private IntListIterator  it;
        //The support for this set (beginning at root)
        protected long support;
        //pointers to children
        protected HashMap<Integer, TrieNode> children = new HashMap<>();
        protected boolean isLeaf; //no children
        protected boolean isFinal; //a sequence ends here (instead of summing and comparing support)
        protected boolean isRelevant; //additional information (used by pattern growth to keep track of relevant outputs)
        protected List<Producer> producers;

        public TrieNode(IntList fids, long support, boolean isFinal, boolean isLeaf, int id) {
            this.support = support;
            this.isLeaf = isLeaf;
            this.isFinal = isFinal;
            this.id = id;
            this.producers = new ArrayList<>();
            this.items = fids;
            this.isRelevant = false;
        }

        // Increment support
        public long incSupport(long s) {
            return this.support += s;
        }

        public int firstItem() {
            if (items.isEmpty()) {
                return -1;
            } else {
                return items.getInt(0);
            }
        }

        //returns previous child trie node (if replaced), else null
        public TrieNode addChild(TrieNode t) {
            return addChild(t.firstItem(), t);
        }

        public TrieNode addChild(int firstItem, TrieNode t) {
            if (isLeaf) isLeaf = false;
            //there must be only one child per next fid
            return children.put(firstItem, t);
        }

        public void addChildren(Map<Integer, TrieNode> map) {
            if (isLeaf) isLeaf = false;
            children.putAll(map);
        }

        public Collection<TrieNode> collectChildren() {
            return children.values();
        }

        public int childrenCount() {
            return children.size();
        }

        public HashMap<Integer, TrieNode> removeAllChildren() {
            HashMap<Integer, TrieNode> removed = children;
            children = new HashMap<>();
            return removed;
        }

        public TrieNode getChildrenStartingWith(int fid) {
            return children.get(fid);
        }

        public IntListIterator itemIterator() {
            if (it == null)
                it = items.iterator();
            return it;
        }

        public IntList getItems(){
            return items;
        }

        public boolean isLeaf(){
            return isLeaf;
        }
        public boolean isFinal(){
            return isFinal;
        }
        public boolean isRelevant(){
            return isRelevant;
        }

        public void setFinal(boolean isFinal){
            this.isFinal = isFinal;
        }


        public long getSupport(){
            return support;
        }

        //Exclusive support (support - sum(children support)
        public long getExclusiveSupport(){
            long childSupport = 0;
            for(TrieNode child: children.values()){
                childSupport += child.support;
            }
            return support - childSupport;
        }

        public int getId(){
            return id;
        }

        public void clearIterator() {
            //force re-init of iterator at next call
            it = null;
        }

        public IntList separateItems(int separatorItem) {
            //avoid inconsistent iterator
            clearIterator();
            int idx = items.indexOf(separatorItem);
            //determine to be removed items (for return) and copy them
            IntList removed = new IntArrayList(items.subList(idx, items.size()));
            //delete remaining based on idx
            items.removeElements(idx, items.size());
            return removed;
        }



        // Handling Pattern Growth

        public void setRelevant(boolean isRelevant){
            this.isRelevant = isRelevant;
        }

        //Try to combine with child
        public void trim(long minSupport, boolean checkRelevant, boolean merge){
            //long supportCorrection = 0;
            if(!children.isEmpty()) {
                IntList prune = new IntArrayList(children.size());
                for (TrieNode child : collectChildren()) {
                    //remove infrequent or not relevant children
                    if (child.support < minSupport) {
                        prune.add(child.firstItem());
                    }
                    else if(checkRelevant && !child.isRelevant() && !child.isFinal()){
                        prune.add(child.firstItem());
                    }
                }
                removeChildrenByFirstItem(prune);


                // --- merge with child?
                //check if not final (would define a sequence end) and has exactly one child
                if (merge && !isFinal && children.size() == 1) {
                    mergeWith(children.values().iterator().next());
                }
            }
            //return supportCorrection;
        }

        public void removeChildrenByFirstItem(IntList remove){
            if(!remove.isEmpty()){
                //Discard references, GC should do the rest
                for(Integer item: remove){
                    TrieNode child = children.remove(item);
                }
            }
            if(children.isEmpty())
                isLeaf = true;

        }

        //Merges two nodes - use with care
        private void mergeWith(TrieNode node){
            //add Items
            items.addAll(node.items);

            //Ensure consistent iterator
            clearIterator();

            //copy (pointers) from child into parent
            isLeaf = node.isLeaf;
            isFinal = node.isFinal;
            isRelevant = node.isRelevant;
            children = node.children;
            producers = node.producers;
            support = node.support;
        }

        public void addProducer(Producer p){
            if(p != null) {
                producers.add(p);
            }
        }


        // Printing Node

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            //builder.append(id);
            builder.append("[");
            for (int i : items) {
                if (!first) builder.append(",");
                builder.append(i);
                if (first) first = false;
            }
            builder.append("]@");
            builder.append(support);
            return builder.toString();
        }

        public String toString(Dictionary dict) {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            //builder.append(id);
            builder.append("[");
            for (String s : dict.sidsOfFids(items)) {
                if (!first) builder.append(",");
                builder.append(s);
                if (first) first = false;
            }
            builder.append("]@");
            builder.append(support);
            return builder.toString();
        }
    }

    public static class Producer{
        protected State state;
        protected PatriciaTrie.TrieNode node;
        protected int itemPos;
        protected long support;
        protected boolean isFinal;
        protected boolean isComplete;

        public Producer(State state, PatriciaTrie.TrieNode node, int itemPos,
                        long support, boolean isFinal, boolean isComplete){
            this.state = state;
            this.node = node;
            this.itemPos = itemPos;
            this.support = support;
            this.isFinal = isFinal;
            this.isComplete = isComplete;
        }

    }
}