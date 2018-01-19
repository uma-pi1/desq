package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import de.uni_mannheim.desq.util.IntBitSet;
import de.uni_mannheim.desq.util.IntByteArrayList;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.commons.io.FilenameUtils;

import java.util.*;

public class PatriciaTrieBasic {

    private TrieNode root;
    private int currentNodeId;
    private ObjectList<TrieNode> nodes;
    private boolean maintainRelationLists;


    public PatriciaTrieBasic() {
        this(false);
    }

    public PatriciaTrieBasic(boolean maintainRelationLists) {
        //init root node with empty list
        this.nodes = new ObjectArrayList<>();
        this.currentNodeId = -1;

        this.maintainRelationLists = maintainRelationLists;

        this.root = new TrieNode(new IntArrayList(), (long) 0,
                false, true, ++currentNodeId, maintainRelationLists);
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
        root.children.clear();
        root = new TrieNode(new IntArrayList(), (long) 0,
                false, true, ++currentNodeId, maintainRelationLists);
        nodes.add(currentNodeId, root);
    }

    public void addItems(IntList fids, long support) {
        addItems(root, fids, support);
    }


    public void addItems(IntList items, long support, int largestFrequentFid) {
        //Prune
        /*for(int i = 0;i < items.size(); i++) {
            if (items.getInt(i) > largestFrequentFid) {
                if (i == 0) return;
                items = items.subList(0, i);
                break;
            }
        }*/
        IntListIterator it = items.listIterator();
        while(it.hasNext())
            if (it.nextInt() > largestFrequentFid) it.remove();

        if(!items.isEmpty())
            addItems(items,support);
    }

    /**
     * Adding fids into the trie by traversing (starting at given start node)
     * @param startNode node of trie to start at
     * @param items to be added
     * @param support support/weight of these items
     */

    public void addItems(TrieNode startNode, IntList items, long support) {
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
                    expandTrie(currentNode, createIntList(currentItem, it),support,true, false);
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
                    expandTrie(currentNode, createIntList(currentItem, it), support, true, false);
                    break; //remaining input added -> finished processing
                } else {
                    //found child node starting with current item
                    //go to next node, but inc support and clean up current
                    currentNode.incSupport(support);
                    currentNode.clearIterator();
                    currentNode = nextNode;
                    //skip the first item (already checked via hash key)
                    currentNode.itemIterator().next();
                    //Case: last item of new input (but node has more!) -> it would end while loop -> handle split
                    if(!it.hasNext() && currentNode.itemIterator().hasNext()){
                        splitNode(currentNode, currentNode.itemIterator().next());
                        //stays final because sequence ends here
                        break;
                    }//if the nodes does not have a next item either -> they are equal -> handling after loop
                    //continue;
                }
            }
        }
        //ensure that support of last visited node is increased and clean up
        //doing it after loop ensures that the last node is considered if there was no expand
        currentNode.incSupport(support);
        currentNode.clearIterator();
    }

    //Construct the remaining items (is there a more efficient way?)
    private IntList createIntList(int firstItem, IntListIterator remainingItems) {
        IntList items = new IntArrayList();
        items.add(firstItem);
        if (remainingItems.hasNext()) { //there are more items to add
            remainingItems.forEachRemaining(items::add);
        }
        return items;
    }

    private TrieNode expandTrie(TrieNode startNode, IntList items, long support, boolean isFinal, boolean moveChildren) {
        //Create new node
        TrieNode newNode = new TrieNode(items, support,isFinal,true, ++currentNodeId, maintainRelationLists);
        //add new node to list
        nodes.add(currentNodeId, newNode);

        //handle insert of node
        if(moveChildren && !startNode.children.isEmpty()){
            //reuse existing list of direct children
            newNode.children = startNode.children;
            //re-init existing list of direct children
            startNode.children = new Int2ObjectOpenHashMap<>();

            //Descendants are equal for now
            //newNode.descendants.addAll(startNode.descendants);
            newNode.descendants = (BitSet) startNode.descendants.clone();
            //Update all descendants
            newNode.resetParentOfChildren();

            newNode.isLeaf = false;
        }
        //copy ancestor information (in any case equal before adding the new node)
        if(!startNode.ancestors.isEmpty()) {
            //newNode.ancestors.addAll(startNode.ancestors);
            newNode.ancestors = (BitSet) startNode.ancestors.clone();
        }
        //Add the child into the trie (and add start node as parent of new Node)
        startNode.addChild(newNode);
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
        //Int2ObjectOpenHashMap<TrieNode> existingChildren = node.removeAllChildren();
        //Int2ObjectOpenHashMap<TrieNode> existingChildren = node.children;
        //node.children = new Int2ObjectOpenHashMap<>();
        //expand from existing node with remaining items
        //if original node is not final, child is neither
        //if original node stays final is decided in addItem node (depends on case)
        TrieNode newNode = expandTrie(node, remaining, node.support, node.isFinal, true);
        /*
        //add existing children to new node
        if (!existingChildren.isEmpty()) {
            newNode.children = existingChildren;
            newNode.isLeaf = false;
        }*/
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
        for (TrieNode child : node.getChildren()) {
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

    public IndexPatriciaTrie convertToIndexBasedTrie(){
        IndexPatriciaTrie trie = new IndexPatriciaTrie(size(), root.getId());
        nodes.parallelStream().forEach(trie::addNode);
        return trie;
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
        protected Int2ObjectOpenHashMap<PatriciaTrieBasic.TrieNode> children = new Int2ObjectOpenHashMap<>();
        protected boolean isLeaf; //no children
        protected boolean isFinal; //a sequence ends here (instead of summing and comparing support)

        protected TrieNode parent; //can only have one parent node -> used for propagation of child info
        protected BitSet ancestors = new BitSet(); // all parents (of parents ...) till root
        protected BitSet descendants = new BitSet(); // all children (of children ...)

        protected boolean maintainRelationLists;

        //interval
        //protected  int intervalStart;
        //protected  int intervalEnd;
        protected IntervalNode intervalNode;




        public TrieNode(IntList fids, long support,
                        boolean isFinal, boolean isLeaf, int id, boolean maintainRelationLists) {
            this.support = support;
            this.isLeaf = isLeaf;
            this.isFinal = isFinal;
            this.id = id;
            this.items = fids;
            this.maintainRelationLists = maintainRelationLists;
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

        public Collection<TrieNode> getChildren() {
            return children.values();
        }

        public AbstractObjectIterator<TrieNode> getChildrenIterator(int largestFrequentFid) {
            return new AbstractObjectIterator<TrieNode>() {
                ObjectIterator<TrieNode> childrenIt = children.values().iterator();
                TrieNode nextChild = move();

                @Override
                public TrieNode next() {
                    assert hasNext();
                    final TrieNode result = nextChild;
                    move();
                    return result;
                }

                @Override
                public boolean hasNext() {
                    return nextChild != null;
                }

                private TrieNode move() {
                    while (childrenIt.hasNext()) {
                        nextChild = childrenIt.next();
                        if (nextChild.firstItem() <= largestFrequentFid)
                            return nextChild;
                    }
                    return (nextChild = null);
                }
            };
        }

        //returns previous child trie node (if replaced), else null
        public TrieNode addChild(TrieNode t) {
            return addChild(t.firstItem(), t);
        }

        public TrieNode addChild(int firstItem, TrieNode t) {
            if (isLeaf) isLeaf = false;
            //maintain reference
            t.parent = this;
            //propagate parent information to all descendants
            t.addAncestor(this.id);
            //propagate new child information to all ancestors
            this.addDescendant(t.id);
            //there must be only one child per next fid
            return children.put(firstItem, t);
        }

        //updates parents add children and triggers update of all descendants
        public void resetParentOfChildren(){
            if(!children.isEmpty()){
                for(TrieNode child: children.values()){
                    child.parent = this;
                    child.addAncestor(this.id);
                }
            }
        }

        public void addAncestor(Integer a){
            if(maintainRelationLists) {
                //propagate ancestor to children (if not already known)
                if (!ancestors.get(a)) {
                    ancestors.set(a);
                    //if(!descendants.contains(a)){
                    //  descendants.add(a);
                    if (!children.isEmpty()) {
                        for (TrieNode child : children.values()) {
                            child.addAncestor(a);
                        }
                    }
                }
            }
        }

        public void addDescendant(Integer d){
            if(maintainRelationLists) {
                //descendants.add(d);
                //propagate new descendant to parent (if not already known)
                if (!descendants.get(d)) {
                    descendants.set(d);
                    //if(!descendants.contains(d)){
                    //  descendants.add(d);
                    if (parent != null) {
                        parent.addDescendant(d);
                    }
                }
            }
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

        public void setFinal(boolean isFinal){
            this.isFinal = isFinal;
        }

        public long getSupport(){
            return support;
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

        /**
         * Method calculating interval tree information (including its children)
         * Returns the highest id used
         */
        public int calculateIntervals(final int start){
            //intervalStart = start;
            if(isLeaf){
                //This node is a leaf -> interval start = end
                //intervalEnd = start;
                intervalNode = new IntervalNode(start,start,support);
                return start;
            }else{
                //Not a leaf -> iterate over all children (depth-first to ensure consistent intervals)
                int end = start;
                int nextStart = start;
                for(TrieNode child: getChildren()) {
                    end = child.calculateIntervals(nextStart);
                    //next start
                    nextStart = end + 1;
                }

                //if(isFinal) end += 1; //but not leaf -> represents end of sequence (higher support than children) -> ensure precedence

                //intervalEnd = end;
                intervalNode = new IntervalNode(start,end,support);
                return end;
            }
        }
        /*
        public void addToIndexBasedTrie(IndexPatriciaTrie trie){
            trie.addNode(this);
            if(!isLeaf){
                for (TrieNode child: getChildren()){
                    child.addToIndexBasedTrie(trie);
                }
            }
        }*/

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

}