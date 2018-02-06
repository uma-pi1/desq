package de.uni_mannheim.desq.mining;
import de.uni_mannheim.desq.experiments.MetricLogger;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.concurrent.atomic.LongAdder;

public class IndexPatriciaTrie {

    private int rootId;  //index of root
    private int size;

    //unchecked array handling!
    private long[] nodeSupport;
    private IntList[] nodeChildren;
    private IntList[] nodeItems;
    private boolean[] nodeIsFinal;
    private boolean[] nodeIsLeaf;
    //private IntervalNode[] nodeInterval;
    private IntervalNode[] nodeIntervalNode;
    private IntervalNode[] nodeFinalIntervalNode;
    private int[] nodeItemsSize;


    public IndexPatriciaTrie(int size, int rootId) {
        this.size = size;
        this.rootId = rootId;

        this.nodeSupport = new long[size];
        this.nodeChildren = new IntList[size];
        this.nodeItems = new IntList[size];
        this.nodeIsFinal = new boolean[size];
        this.nodeIsLeaf = new boolean[size];
        //this.nodeInterval = new IntervalNode[size];
        this.nodeIntervalNode = new IntervalNode[size];
        this.nodeFinalIntervalNode = new IntervalNode[size];
        this.nodeItemsSize = new int[size];

    }

    //Copy trie node to index based list based on trie node id
    public void addNode(PatriciaTrie.TrieNode node) {
        //Node Id
        int idx = node.getId();

        //Fill values with corresponding node index
        nodeItems[idx] =  node.items;
        nodeSupport[idx] =  node.support;
        nodeIsFinal[idx] = node.isFinal;
        nodeIsLeaf[idx] =  node.isLeaf;
        nodeIntervalNode[idx] = new IntervalNode(
                node.intervalStart, node.intervalEnd,
                node.support, node.exclusiveSupport, false);
        nodeFinalIntervalNode[idx] = new IntervalNode(
                node.intervalStart, node.intervalEnd,
                node.support, node.exclusiveSupport, true);
        nodeItemsSize[idx] = node.items.size();

        //convert children
        IntList childrenList = new IntArrayList(node.children.size());
        node.children.values().forEach((trieNode) -> childrenList.add(trieNode.getId()));
        this.nodeChildren[idx] = childrenList;
    }

    //getter
    public int getRootId() {
        return rootId;
    }

    public long getSupport(int nodeId) {
        return nodeSupport[nodeId];
    }


    public boolean isFinal(int nodeId) {
        return nodeIsFinal[nodeId];
    }

    public boolean isLeaf(int nodeId) {
        return nodeIsLeaf[nodeId];
    }

    public IntList getItems(int nodeId) {
        return nodeItems[nodeId];
    }

    public int getItemsSize(int nodeId){
        return nodeItemsSize[nodeId];
    }

    public int getItem(int nodeId, int itemIdx) {
        return nodeItems[nodeId].getInt(itemIdx);
    }

    public IntList getChildren(int nodeId) {
        return nodeChildren[nodeId];
    }

    public IntervalNode getIntervalNode(int nodeId, boolean isFinalComplete) {
        return (isFinalComplete) ? nodeFinalIntervalNode[nodeId] : nodeIntervalNode[nodeId];
    }


    //Measuring KPIs
    public void calcMetrics(){
        MetricLogger logger = MetricLogger.getInstance();
        logger.add(MetricLogger.Metric.NumberInputTrieNodes,size);

        //calc conditionals
        LongAdder leafNodes = new LongAdder();
        LongAdder finalNodes = new LongAdder();
        LongAdder itemsLength = new LongAdder();
        LongAdder itemsLengthFinal = new LongAdder();
        LongAdder itemsLengthLeaf = new LongAdder();
        LongAdder itemsLengthWeighted = new LongAdder();
        LongAdder itemsLengthFinalWeighted = new LongAdder();
        LongAdder itemsLengthLeafWeighted = new LongAdder();

        for(int i = rootId; i < size;i++){
            int size = nodeItemsSize[i];
            long sizeWeighted = nodeSupport[i] * size;
            itemsLength.add(size);
            itemsLengthWeighted.add(sizeWeighted);
            if(nodeIsLeaf[i]){
                leafNodes.add(1);
                itemsLengthLeaf.add(size);
                itemsLengthWeighted.add(sizeWeighted);
            }
            if(nodeIsFinal[i]){
                finalNodes.add(1);
                itemsLengthFinal.add(size);
                itemsLengthFinalWeighted.add(sizeWeighted);
            }
        }
        logger.add(MetricLogger.Metric.LengthOfItems,itemsLength.longValue());
        logger.add(MetricLogger.Metric.NumberInputTrieLeafNodes,leafNodes.longValue());
        logger.add(MetricLogger.Metric.NumberInputTrieFinalNodes,finalNodes.longValue());
        logger.add(MetricLogger.Metric.LeafNodesLengthOfItems,itemsLengthLeaf.longValue());
        logger.add(MetricLogger.Metric.FinalNodesLengthOfItems,itemsLengthFinal.longValue());
        logger.add(MetricLogger.Metric.LengthOfItemsWeighted,itemsLengthWeighted.longValue());
        logger.add(MetricLogger.Metric.LeafNodesLengthOfItemsWeighted,itemsLengthLeafWeighted.longValue());
        logger.add(MetricLogger.Metric.FinalNodesLengthOfItemsWeighted,itemsLengthFinalWeighted.longValue());
    }


    public int size() {
        return size;
    }

    public void clear() {
        this.size = -1;
        this.rootId = -1;

        this.nodeSupport = null;
        this.nodeChildren = null;
        this.nodeItems = null;
        this.nodeIsFinal = null;
        this.nodeIsLeaf = null;
        this.nodeFinalIntervalNode = null;
        this.nodeIntervalNode = null;
    }

    /*  =============== Potentially useful outputs ===========================
    public String getNodeString(int nodeId, Dictionary dict) {
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

    /**
     * Exports the trie using graphviz (type bsed on extension, e.g., "gv" (source file), "pdf", ...)
     *
    public void exportGraphViz(String file, int maxDepth, Dictionary dict) {
        AutomatonVisualizer automatonVisualizer = new AutomatonVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        automatonVisualizer.beginGraph();
        expandGraphViz(rootId, automatonVisualizer, maxDepth, dict);
        //automatonVisualizer.endGraph(trie.getRoot().toString());
        automatonVisualizer.endGraph();
    }

    private void expandGraphViz(int nodeId, AutomatonVisualizer viz, int depth, Dictionary dict) {
        int nextDepth = depth - 1;
        for (int childId : getChildren(nodeId)) {
            //viz.add(node.toString(), String.valueOf(child.firstItem()), child.toString());
            viz.add(String.valueOf(nodeId), getNodeString(childId, dict), String.valueOf(childId));
            if (isFinal(childId)) {
                //viz.addFinalState(child.toString());
                viz.addFinalState(String.valueOf(childId), isLeaf(childId));
            }
            if (!isLeaf(childId) && depth > 0) {
                expandGraphViz(childId, viz, nextDepth, dict);
            }
        }
    }
    */
}