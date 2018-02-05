package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.experiments.MetricLogger;
import de.uni_mannheim.desq.fst.graphviz.AutomatonVisualizer;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.commons.io.FilenameUtils;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/** Patricia Trie data structure
 * Implementing the operations on the trie and keeps reference to the root node
 **/
public class PatriciaTrie {

    /** root node of the trie */
    private TrieNode root;

    /** used for continuous ids of node (increment with each added) */
    private int currentNodeId;

    /** storing references to each node by its id (=index) */
    private ObjectList<TrieNode> nodes;

    /** init trie with empty root node */
    public PatriciaTrie() {

        this.nodes = new ObjectArrayList<>();
        this.currentNodeId = -1;

        //init root node with empty list
        this.root = new TrieNode(new IntArrayList(), (long) 0,
                false, true, ++currentNodeId);
        nodes.add(currentNodeId, root);
    }

    public void clear(){
        nodes.clear();
        currentNodeId = -1;
        root.children.clear();
        root = new TrieNode(new IntArrayList(), (long) 0,
                false, true, ++currentNodeId);
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



    public void addItems(IntList fids, long support) {
        addItems(root, fids, support);
    }

    /**
     * Adding items into the trie by traversing the trie (starting at given start node)
     * Compares the given list with nodes in the trie and increases the support of visited nodes
     * Potentially a new now is added to the trie as well as an existing one split
     * @param startNode node of trie to start at
     * @param items to be added
     * @param support support/weight of these items
     */
    public void addItems(TrieNode startNode, IntList items, long support) {
        //traverse trie and inc support till nodes do not match or anymore or end of list
        IntListIterator inputIt = items.iterator();
        IntListIterator nodeIt = startNode.items.iterator();
        TrieNode currentNode = startNode;

        while (inputIt.hasNext()) { //iterate over input items
            int currentItem = inputIt.next();
            // Compare input with existing items in node
            if (nodeIt.hasNext()) { //and iterate in parallel over node items
                //compare next item in current node
                int nodeItem = nodeIt.next();
                //Case: item list differs -> split in common prefix and extend new sequence part
                if (currentItem != nodeItem) {
                    //node item and input item differ -> split node and extend with remaining!
                    splitNode(currentNode, nodeIt.previousIndex());
                    currentNode.setFinal(false); //only sub-sequence which splits (has multiple children)
                    //and add new node with remaining input items (at least one)
                    expandTrie(currentNode, createIntList(currentItem, inputIt),support,true, false);
                    break; //remaining input added -> finished processing
                } //else: same item in input and in node so far

                //Case: item list is shorter than node Items -> split and update the first part (current node)
                else if(!inputIt.hasNext() && nodeIt.hasNext()) {
                    splitNode(currentNode, nodeIt.nextIndex());
                    currentNode.setFinal(true);//sequence ends here
                    break;
                }
                //Case: sequence fits in existing trie
                else if(!inputIt.hasNext() && !nodeIt.hasNext()){
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
                    expandTrie(currentNode, createIntList(currentItem, inputIt), support, true, false);
                    break; //remaining input added -> finished processing
                } else {
                    //found child node starting with current item
                    //go to next node, but inc support and clean up current
                    currentNode.incSupport(support);
                    //currentNode.clearIterator();
                    nodeIt = nextNode.items.iterator();
                    currentNode = nextNode;
                    //skip the first item (already checked via hash key)
                    nodeIt.next();
                    if(!inputIt.hasNext()){//last item of input -> loop will end
                        if(nodeIt.hasNext()){
                            //Case: last item of new input (but node has more!)
                            splitNode(currentNode, nodeIt.nextIndex());
                        }
                        currentNode.setFinal(true); //sequence ends here
                        break;
                    }
                    //continue;
                }
            }
        }
        //ensure that support of last visited node is increased and clean up
        //doing it after loop ensures that the last node is considered if there was no expand
        currentNode.incSupport(support);
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

    /**
     * Append a new node after a give one.
     * @param startNode existing parent of the new node
     * @param items items of the new node
     * @param support support of the new node
     * @param isFinal is the new node end of an input sequence?
     * @param moveChildren Whether or not the children are moved from the start to the new node (split case)
     * @return the new node
     */
    private TrieNode expandTrie(TrieNode startNode, IntList items, long support, boolean isFinal, boolean moveChildren) {
        //Create new node
        TrieNode newNode = new TrieNode(items, support,isFinal,true, ++currentNodeId);
        //add new node to list
        nodes.add(currentNodeId, newNode);

        //handle insert of node in existing paths
        if(moveChildren && !startNode.children.isEmpty()){
            //reuse existing list of direct children
            newNode.children = startNode.children;
            //re-init existing list of direct children
            startNode.children = new Int2ObjectOpenHashMap<>();

            newNode.isLeaf = false;
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
     * @param idx index of first item of new (second) node
     * @return the new node
     */
    private TrieNode splitNode(TrieNode node, int idx) {
        IntList remaining = node.separateItems(idx);
        //remove children pointer from existing node
        //expand from existing node with remaining items
        //if original node is not final, child is neither
        //if original node stays final is decided in addItem method (depends on case)
        TrieNode newNode = expandTrie(node, remaining, node.support, node.isFinal, true);
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

    //Measuring KPIs
    public void calcMetrics(){
        MetricLogger logger = MetricLogger.getInstance();
        logger.add(MetricLogger.Metric.NumberInputTrieNodes,nodes.size());

        //calc conditionals
        LongAdder leafNodes = new LongAdder();
        LongAdder finalNodes = new LongAdder();
        LongAdder itemsLength = new LongAdder();
        LongAdder itemsLengthFinal = new LongAdder();
        LongAdder itemsLengthLeaf = new LongAdder();
        LongAdder itemsLengthWeighted = new LongAdder();
        LongAdder itemsLengthFinalWeighted = new LongAdder();
        LongAdder itemsLengthLeafWeighted = new LongAdder();
        nodes.parallelStream().forEach(node -> {
            int size = node.items.size();
            long sizeWeighted = node.support * size;
            itemsLength.add(size);
            itemsLengthWeighted.add(sizeWeighted);
            if(node.isLeaf()) {
                leafNodes.add(1);
                itemsLengthLeaf.add(size);
                itemsLengthLeafWeighted.add(sizeWeighted);
            }
            if(node.isFinal()) {
                finalNodes.add(1);
                itemsLengthFinal.add(size);
                itemsLengthFinalWeighted.add(sizeWeighted);
            }
        });
        logger.add(MetricLogger.Metric.LengthOfItems,itemsLength.longValue());
        logger.add(MetricLogger.Metric.NumberInputTrieLeafNodes,leafNodes.longValue());
        logger.add(MetricLogger.Metric.NumberInputTrieFinalNodes,finalNodes.longValue());
        logger.add(MetricLogger.Metric.LeafNodesLengthOfItems,itemsLengthLeaf.longValue());
        logger.add(MetricLogger.Metric.FinalNodesLengthOfItems,itemsLengthFinal.longValue());
        logger.add(MetricLogger.Metric.LengthOfItemsWeighted,itemsLengthWeighted.longValue());
        logger.add(MetricLogger.Metric.LeafNodesLengthOfItemsWeighted,itemsLengthLeafWeighted.longValue());
        logger.add(MetricLogger.Metric.FinalNodesLengthOfItemsWeighted,itemsLengthFinalWeighted.longValue());
    }

    // ------------------------- TRIE NODE -----------------------------

    public class TrieNode {
        // unique id = index
        private int id;
        //The sequence of items (as int values) e.g. FIDs
        protected IntList items;
        //The support values for this sequence
        protected long support;
        protected long exclusiveSupport;

        //Referencing the children via their first item
        protected Int2ObjectMap<TrieNode> children;
        //Flags
        protected boolean isLeaf; //no children
        protected boolean isFinal; //a sequence ends here
        //Interval information
        protected int intervalStart;
        protected int intervalEnd;


        public TrieNode(IntList fids, long support,
                        boolean isFinal, boolean isLeaf, int id) {
            this.support = support;
            this.isLeaf = isLeaf;
            this.isFinal = isFinal;
            this.id = id;
            this.items = fids;
            this.children = new Int2ObjectOpenHashMap<>();
        }

        /// METHODS for building the trie

        /** Increment the support by s
         * @return the incremented support value
         */
        public long incSupport(long s) {
            return this.support += s;
        }

                /**
         * Add a child node
         * @return replaced child node, if there was one (should no happen!)
         */
        public TrieNode addChild(TrieNode t) {
            return addChild(t.firstItem(), t);
        }

        /**
         * Add a child node, providing the the first item (it is mapped to)
         * @return replaced child node, if there was one (should no happen!)
         */
        public TrieNode addChild(int firstItem, TrieNode t) {
            if (isLeaf) isLeaf = false;
            //there must be only one child per next fid
            return children.put(firstItem, t);
        }

        /**
         * Separating all items after a given index
         * @return removed items
         */
        public IntList separateItems(int idx) {
            //determine to be removed items (for return) and copy them
            IntList removed = new IntArrayList(items.subList(idx, items.size()));
            //delete remaining based on idx
            items.removeElements(idx, items.size());
            return removed;
        }

        public void setFinal(boolean isFinal){
            this.isFinal = isFinal;
        }


        /**
         * Method calculating interval tree information (including its children)
         * Returns the highest id used
         */
        public int calculateIntervals(final int start){
            intervalStart = start;
            if(isLeaf){
                //This node is a leaf -> interval start = end
                intervalEnd = start;
                exclusiveSupport = support;
                return start;
            }else{
                //Not a leaf -> iterate over all children (depth-first to ensure consistent intervals)
                int end = start;
                int nextStart = start;
                LongAdder childrenSupport = new LongAdder();
                for(TrieNode child: getChildren()) {
                    end = child.calculateIntervals(nextStart);
                    //next start
                    nextStart = end + 1;
                    //track child supports
                    childrenSupport.add(child.support);
                }
                intervalEnd = end;
                //intervalNode = new IntervalNode(start,end,support);
                exclusiveSupport = support - childrenSupport.longValue();
                return end;
            }
        }

        //// GETTER
        /** Get the first item
         * @return -1 if empty list of item, else the first item
         */
        public int firstItem() {
            if (items.isEmpty()) {
                return -1;
            } else {
                return items.getInt(0);
            }
        }
        /**
         * Get collection of children (not the hash map)
         * @return collection of children
         */
        public Collection<TrieNode> getChildren() {
            return children.values();
        }

        /**
         * Get child node starting with first item
         * @return Node if present, else null
         */
        public TrieNode getChildrenStartingWith(int fid) {
            return children.get(fid);
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

        public long getSupport(){
            return support;
        }

        public int getId(){
            return id;
        }



        ////  Printing Node
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
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