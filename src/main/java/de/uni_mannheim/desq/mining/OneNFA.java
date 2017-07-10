package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import de.uni_mannheim.desq.util.BrzozowskiDatastructures;
import de.uni_mannheim.desq.util.PrimitiveUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.FilenameUtils;

import java.util.BitSet;


/**
 * One large NFA which holds all accepting paths through the FST for one input sequence
 * After constructing this NFA, we extract the (partial) NFAs for each pivot from this NFA
 *
 * Each state of this NFA can be accessed by one of two indexing schemes:
 * External indexing: (q, pos): q is the FST state, pos is the last-read position after arriving at this state
 * Internal indexing: s: s is an internal state id
 * sByQp is a map that translates from external to internal indexing: (q, pos) -> s
 */
public class OneNFA {
    /** Maps from external index (q, pos) to internal state id s */
     Long2IntOpenHashMap sByQp = new Long2IntOpenHashMap();

    /** Holds the forward edges for each state  */
    ObjectArrayList<Object2ObjectOpenHashMap<OutputLabel, IntSet>> forwardEdges = new ObjectArrayList<>();

    /** Holds the backward edges for each state */
    ObjectArrayList<Object2ObjectOpenHashMap<OutputLabel, IntSet>> backwardEdges = new ObjectArrayList<>();

    /** Holds the maximum (pivot) item seen in the path between state s and the initial state */
    IntArrayList maxPivot = new IntArrayList();

    /** A set of all final states */
    LongSet isFinal = new LongOpenHashSet();

    /** A set of states that have no following output (used to prevent unnecessary trailing eps-edges) */
    LongSet noFollowingOutput = new LongOpenHashSet();

    /** A set of all (pos,q) pairs that we have found to be a dead end. Is indexed by a long[q,pos], so we don't need to create a state for every dead end*/
    LongOpenHashSet isDeadEnd = new LongOpenHashSet();

    /** We cache our output labels, so that we can use identical objects if one OutputLabel occurs multiple times  */
    Object2ObjectOpenHashMap<IntArrayList,OutputLabel> outputLabelCache = new Object2ObjectOpenHashMap<>();

    /** Maximum FST state q and position, for which we have a state */
    int maxPos = 0;
    int maxQ = 0;

    /** Data structures for determinizing the NFA */
    BrzozowskiDatastructures bz = new BrzozowskiDatastructures(this);

    /** Processed and to-process states for finding outgoing edges of a set of states */
    BitSet alreadyProcessed;
    BitSet toProcess;

    public void clear() {
        forwardEdges.clear();
        backwardEdges.clear();
        maxPivot.clear();
        isDeadEnd.clear();
        isFinal.clear();
        sByQp.clear();
        maxPos = 0;
        maxQ = 0;
        outputLabelCache.clear();
        noFollowingOutput.clear();
    }

    /** Checks whether edge (qFrom,pos) --> (qTo,pos+1) with OutputLabel `label` exists. */
    public boolean checkForEdge(int qFrom, int pos, int qTo, OutputLabel label) {
        // get interal state id by combining (q,pos) into a long, then looking up in sByQp
        long spFrom = PrimitiveUtils.combine(qFrom, pos);
        int s = sByQp.getOrDefault(spFrom, -1);

        // if this state does not exist, the edge does not exist
        if(s == -1)
            return false;

        // look for an edge with this label
        IntSet toStates = forwardEdges.get(s).getOrDefault(label, null);
        if(toStates == null) // no edge with this label
            return false;

        // look up the internal id of the to-state
        int sTo = checkForState(qTo, pos+1);
        if(sTo == -1) // if the state does not exists, there can also be no edge to it
            return false;

        // check whether the to-state is in the set of to-states for this edge
        return toStates.contains(sTo);

    }

    /** Checks whether state (q, pos) exists. Returns the state id if it does, -1 otherwise */
    public int checkForState(int q, int pos) {
        long sp = PrimitiveUtils.combine(q, pos);
        return sByQp.getOrDefault(sp, -1);
    }

    public int checkForState(long qp) {
        return sByQp.getOrDefault(qp, -1);
    }

    /** Adds an edge (qFrom,pos) --> (qTo,pos+1) with output label `label`. */
    public void addEdge(int qFrom, int pos, int qTo, OutputLabel label) {
        // if we know that the to-state does not produce any output and we have an eps output label, we don't add this edge
        if(label == null && hasNoFollowingOutput(qTo, pos+1)) {
            if(isFinal(qTo, pos+1)) { // TODO: can probably drop this
                // if that state is final though, we need to mark the current state as final
                markFinal(qFrom, pos);
            }
            return;
        }

        // creates the states if they do not exist yet
        int sFrom = getOrCreateState(qFrom, pos); // if this state doesn't exist yet, it will be created
//        assert(checkForState(qTo, pos+1) != -1): "State " + qTo + "," + (pos+1) + " does not exist yet. But it should.";
        int sTo = getOrCreateState(qTo, pos+1); // the to-state already exists, as we build back->front

        // add forward edge sFrom->sTo
        IntSet toStates = forwardEdges.get(sFrom).getOrDefault(label, null);
        if (toStates == null) {
            toStates = new IntOpenHashSet();
            forwardEdges.get(sFrom).put(label, toStates);
        }
        toStates.add(sTo);


        // add backwards edge sTo->sFrom
        toStates = backwardEdges.get(sTo).getOrDefault(label, null);
        if (toStates == null) {
            toStates = new IntOpenHashSet();
            backwardEdges.get(sTo).put(label, toStates);
        }
        toStates.add(sFrom);

        // update maxPivot of the to-state (and propagate the change forwards if necessary):
        // maxPivot~the maximum item seen in the path from this state to the initial state
        int newPivot = Math.max(maxPivot.getInt(sFrom), label == null ? -1 : label.getMaxOutputItem());
        propagateNewPivotItem(newPivot, sTo);
    }

    /** Propagates a pivot item forwards (towards the final state(s)), starting from state s */
    private void propagateNewPivotItem(int pivot, int s) {
        // if the given pivot is larger than the currently stored one, we store it and propagate it forwards
        if(pivot > maxPivot.getInt(s)) {
            // store the new maxPivot
            maxPivot.set(s, pivot);

            // propagate to all forward paths
            for(IntSet prevStates : forwardEdges.get(s).values()) {
                for(int sNext : prevStates) {
                    propagateNewPivotItem(pivot, sNext);
                }
            }
        }
    }

    /** Returns internal state id s for given (q,pos). If such a state doesn't exist yet, it is created */
    private int getOrCreateState(int q, int pos) {
        long qp = PrimitiveUtils.combine(q, pos);
        int s = sByQp.getOrDefault(qp, -1);

        // state does not exist yet, so we create it
        if(s == -1) {
            s = sByQp.size();
            sByQp.put(qp, s);
            maxPos = Math.max(maxPos, pos);
            maxQ = Math.max(maxQ, q);

            forwardEdges.add(new Object2ObjectOpenHashMap<>());
            backwardEdges.add(new Object2ObjectOpenHashMap<>());

            maxPivot.add(-1);
        }
        return s;
    }

    /** Returns an OutputLabel object for the given transition, input fid and set of output items. Reuses an existing
     * OutputLabel object if possible.
     */
    public OutputLabel getOrCreateLabel(Transition tr, int inputItemFid, IntArrayList outputItems) {
        // check for such a label
        OutputLabel label = outputLabelCache.getOrDefault(outputItems, null);

        // we don't have one yet, so we create a new one
        if (label == null) {
            label = new OutputLabel(tr, inputItemFid, outputItems);
            outputLabelCache.put(outputItems, label);
        }
        return label;
    }

    /** Mark the state (q,pos) as final. Creates a state for (q,pos) if it doesn't exist yet. */
    public void markFinal(int q, int pos) {
        long l = PrimitiveUtils.combine(q, pos);
        isFinal.add(l);
    }

    /** Check whether state (q,pos) is marked as final */
    public boolean isFinal(int q, int pos) {
        long l = PrimitiveUtils.combine(q, pos);
        return isFinal.contains(l);
    }

    public boolean hasNoFollowingOutput(int q, int pos) {
        long l = PrimitiveUtils.combine(q, pos);
        return noFollowingOutput.contains(l);
    }
    public boolean markNoFollowingOutput(int q, int pos) {
        long l = PrimitiveUtils.combine(q, pos);
        return noFollowingOutput.add(l);
    }


    /** Check whether state s is an initial state */
    public boolean isInitial(int s) {
        return maxPivot.getInt(s) == -1;
    }

    /** Mark state (q,pos) as dead end. Does not create a state. */
    public void markDeadEnd(int q, int pos) {
        isDeadEnd.add(PrimitiveUtils.combine(q, pos));
    }

    /** Check whether state (q,pos) is a dead end */
    public boolean isDeadEnd(int q, int pos) {
        return isDeadEnd.contains(PrimitiveUtils.combine(q, pos));
    }

    /** Returns the maximum pivot for state s */
    public int getMaxPivot(int s) {
        return maxPivot.getInt(s);
    }

    /** Returns the number of states in this NFA */
    public int numStates() {
        return sByQp.size();
    }

    public IntSet getPivotsForward() {
        IntOpenHashSet pivots = new IntOpenHashSet();

        ObjectArrayList<IntAVLTreeSet> potentialPivots = new ObjectArrayList<>();

        boolean output = false;

        for(int s=0; s<=numStates(); s++) {
            potentialPivots.add(new IntAVLTreeSet());
        }

        for(int pos = 0; pos<=maxPos; pos++) {
            for(int q = 0; q<=maxQ; q++) {
                int s = checkForState(q, pos);
                if(output) System.out.println(q + "," + pos + ": s=" + s);


                if(s != -1) {
                    if(isFinal(q,pos)) {
                        if(output) System.out.println("is final. adding " + potentialPivots.get(s));
                        pivots.addAll(potentialPivots.get(s));
                    }
                    if(output) System.out.println("Has potential pivots: " + potentialPivots.get(s));
                    for(Object2ObjectMap.Entry<OutputLabel, IntSet> edge : forwardEdges.get(s).object2ObjectEntrySet()) {
                        OutputLabel ol = edge.getKey();
                        int trimMin = Math.max(ol == null ? 0 : ol.outputItems.getInt(0), potentialPivots.get(s).size() > 0 ? potentialPivots.get(s).firstInt() : 0);
                        for(int sTo : edge.getValue()) {
                            if(output) System.out.println("has outgoing edge " + ol + " (trim=" + trimMin +") to state " + sTo);
                            if(ol != null)
                                for(int item : ol.outputItems)
                                    if(item >= trimMin) {
                                        potentialPivots.get(sTo).add(item);
                                        if(output) System.out.println("Adding " + item);
                                    }
                            if(potentialPivots.get(s).size() > 0)
                                for(int item : potentialPivots.get(s))
                                    if(item >= trimMin) {
                                        potentialPivots.get(sTo).add(item);
                                        if(output) System.out.println("Adding " + item);
                                    }
                        }
                    }
                }
            }
        }
        return pivots;
    }

//    IntArrayList outputItems = new IntArrayList();
//    public IntSet getPivotsForwardSlim() {
//        IntOpenHashSet pivots = new IntOpenHashSet();
//
//        ObjectArrayList<IntAVLTreeSet> potentialPivots = new ObjectArrayList<>();
//
//
//        boolean output = false;
//        outputItems.clear();
//
//        for(int s=0; s<=numStates(); s++) {
//            potentialPivots.add(new IntAVLTreeSet()); // reuse this
//        }
//
//        for(int pos = 0; pos<=maxPos; pos++) {
//            for(int q = 0; q<=maxQ; q++) {
//                int s = checkForState(q, pos);
//                if(output) System.out.println(q + "," + pos + ": s=" + s);
//
//
//                if(s != -1) {
//                    if(isFinal(q,pos)) {
//                        if(output) System.out.println("is final. adding " + potentialPivots.get(s));
//                        pivots.addAll(potentialPivots.get(s));
//                    }
//                    if(output) System.out.println("Has potential pivots: " + potentialPivots.get(s));
//                    for(Long2ObjectMap.Entry<IntSet> edge : pivotForwardEdges.get(s).long2ObjectEntrySet()) {
//                        long label = edge.getLongKey();
//                        int trId = PrimitiveUtils.getLeft(label);
//                        int itemFid = PrimitiveUtils.getRight(label);
//                        outputItems = getOutputItems(trId, itemFid, outputItems);
//                        int trimMin = Math.max(outputItems.size() == 0 ? 0 : outputItems.getInt(0), potentialPivots.get(s).size() > 0 ? potentialPivots.get(s).firstInt() : 0);
//                        for(int sTo : edge.getValue()) {
//                            if(output) System.out.println("has outgoing edge " + outputItems + " (trim=" + trimMin +") to state " + sTo);
//                            if(outputItems.size() != 0)
//                                for(int item : outputItems)
//                                    if(item >= trimMin) {
//                                        potentialPivots.get(sTo).add(item);
//                                        if(output) System.out.println("Adding " + item);
//                                    }
//                            if(potentialPivots.get(s).size() > 0)
//                                for(int item : potentialPivots.get(s))
//                                    if(item >= trimMin) {
//                                        potentialPivots.get(sTo).add(item);
//                                        if(output) System.out.println("Adding " + item);
//                                    }
//                        }
//                    }
//                }
//            }
//        }
//        return pivots;
//    }

//    Transition.ItemStateIteratorCache itCache = null;
//
//    public IntArrayList getOutputItems(int trId, int itemFid, IntArrayList outputItems) {
//
//        if(itCache == null)
//            itCache = new Transition.ItemStateIteratorCache(miner.ctx.dict.isForest());
//
//        if(outputItems == null) {
//            outputItems = new IntArrayList();
//        } else {
//            outputItems.clear();
//        }
//
//        // eps-transition
//        if(trId == -1)
//            return outputItems;
//
//        Transition tr = miner.getTrByTrId(trId);
//
//        if (tr.hasOutput()) { // this transition doesn't produce output
//            // collect all output elements into the outputItems set
//            Iterator<ItemState> outIt = tr.consume(itemFid, itCache);
//
//            // we assume that we get the items sorted, in decreasing order
//            // if that is not the case, we need to sort
//            boolean needToSort = false;
//            int lastOutputItem = Integer.MAX_VALUE;
//            int outputItem;
//
//            while (outIt.hasNext()) {
//                outputItem = outIt.next().itemFid;
//
//                assert outputItem != lastOutputItem; // We assume we get each item only once
//                if (outputItem > lastOutputItem)
//                    needToSort = true;
//                lastOutputItem = outputItem;
//
//                // we are only interested in frequent output items
//                if (miner.getLargestFrequentFid() >= outputItem) {
//                    outputItems.add(outputItem);
//                }
//            }
//
//            // if we have frequent output items, build the output label for this transition and follow it
//            if (outputItems.size() > 0) {
//
//                // if we need to sort, we sort. Otherwise we just reverse the elements to have them in increasing order
//                if (needToSort)
//                    IntArrays.quickSort(outputItems.elements(), 0, outputItems.size());
//                else
//                    IntArrays.reverse(outputItems.elements(), 0, outputItems.size());
//            }
//        }
//        return outputItems;
//    }

    /** Determinizes the NFA backwards into <code>this.bz</code> */
    public void determinizeBackwards() {
        bz.clear();

        IntSet finalStates = new IntOpenHashSet();
        for(long l : isFinal) {
            int s = checkForState(l);
            if(s != -1)
                finalStates.add(s);
        }
        int currentState = bz.addNewState(finalStates);

        // outgoing edges of the current state
        Object2ObjectOpenHashMap<OutputLabel, IntSet> outgoingEdges = new Object2ObjectOpenHashMap<>();
        alreadyProcessed = new BitSet(numStates());
        toProcess = new BitSet(numStates());

        // process each newly created state once until there are no newly created states left
        do {
            outgoingEdges.clear();
            // collect the outgoing (backwards) edges of all original states included in this new state
            //    and the states each edge can lead to

            // the outgoing edges of which states are relevant for this new state?
            outgoingEdges = collectOutgoingEdges(bz.getIncludedOriginalStates(currentState));

            // add outgoing edges to the current state
            for(Object2ObjectMap.Entry<OutputLabel, IntSet> outgoingEdge : outgoingEdges.object2ObjectEntrySet()) {
                int toState = bz.getOrCreateState(outgoingEdge.getValue());
                bz.addEdge(currentState, outgoingEdge.getKey(), toState);
            }
            currentState++;
        } while (currentState < bz.numStates());
    }


    /** Collects the outgoing edges for a given set of states */
    private Object2ObjectOpenHashMap<OutputLabel, IntSet> collectOutgoingEdges(IntSet includedStates) {
        Object2ObjectOpenHashMap<OutputLabel, IntSet> outgoingEdges = new Object2ObjectOpenHashMap<>();

        alreadyProcessed.clear();
        toProcess.clear();
        for(int s : includedStates)
            toProcess.set(s);

        while(toProcess.cardinality() != 0) {
            int state = toProcess.nextSetBit(0);
            toProcess.clear(state);
            if(alreadyProcessed.get(state)) {
                continue;
            }
            alreadyProcessed.set(state);

            for(Object2ObjectMap.Entry<OutputLabel, IntSet> edge : backwardEdges.get(state).object2ObjectEntrySet()) {
                if (edge.getKey() != null) {
                    // this edge produces output, so we merge it into the existing map
                    IntSet toStates = outgoingEdges.getOrDefault(edge.getKey(), null);
                    if (toStates == null) {
                        // an edge with this label does not exist yet, so we add it
                        toStates = new IntOpenHashSet(edge.getValue());
                        outgoingEdges.put(edge.getKey(), toStates);
                    } else {
                        // if an edge with this label does already exist, we just add the new to-states
                        toStates.addAll(edge.getValue());
                    }
                } else {
                    // this edge does not produce output, so we follow the transitions to the following states
                    for(int sTo : edge.getValue()) {
                        if(!alreadyProcessed.get(sTo)) {
                            toProcess.set(sTo);
                        } else {
                        }
                    }
                }
            }
        }
        return outgoingEdges;
    }

    /** Finds the pivots in the backwards-determinized NFA */
    public IntArrayList getPivotsBz(IntList seq) {
        return bz.findPivots(seq);
    }

    public void print(Fst fst) {

        for(int q=-1; q<=maxQ; q++) {
            if(q==-1)
                System.out.print("    ");
            else
                System.out.print(q + (fst.getState(q).isFinal() ? (fst.getState(q).isFinalComplete() ? "c" : "f") : " ") + ": " );
            for(int pos=0; pos<=maxPos; pos++) {
                if(q==-1)
                    System.out.print(pos);
                else if(isDeadEnd(q, pos))
                    System.out.print("x");
                else if(checkForState(q, pos) != -1)
                    System.out.print("o");
                else
                    System.out.print(" ");
                System.out.print(" ");
            }
            System.out.println("");
        }
//        System.out.println("q, p -> s -- maxPiv");
//        for(Long2IntOpenHashMap.Entry entry : sByQp.long2IntEntrySet()) {
//            System.out.println(PrimitiveUtils.getLeft(entry.getLongKey()) + ", " + PrimitiveUtils.getRight(entry.getLongKey()) + " -> " + entry.getIntValue() + " -- " + maxPivot.getInt(entry.getIntValue()));
//        }

        System.out.println("");
    }

    /** Export this NFA to PDF */
    public void exportGraphViz(String file) {
        FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        fstVisualizer.beginGraph();
        for(Long2IntOpenHashMap.Entry state : sByQp.long2IntEntrySet()) {
            int q = PrimitiveUtils.getLeft(state.getLongKey());
            int pos = PrimitiveUtils.getRight(state.getLongKey());
            int s = state.getIntValue();

            for (Object2ObjectMap.Entry<OutputLabel, IntSet> trEntry : forwardEdges.get(s).object2ObjectEntrySet()) {
                OutputLabel ol = trEntry.getKey();
                String label;
                if(ol == null) {
                    label = "eps";
                } else {
                    label = ol.outputItems.toString() + "(" + ol.inputItem + ")";
                }
                for(int sTo : trEntry.getValue()) {
                    fstVisualizer.add(String.valueOf(s), label, String.valueOf(sTo));
                }
            }
            if (isFinal(q,pos))
                fstVisualizer.addFinalState(String.valueOf(getOrCreateState(q,pos)));
        }
        fstVisualizer.endGraph();
    }

    /** Export all states of the Brzozowski data structure to PDF */
    public void exportBdWithGraphViz(String file) {
        FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        fstVisualizer.beginGraph();
        for (int s = 0; s < bz.numStates(); s++) {
            for (Object2IntMap.Entry<OutputLabel> trEntry : bz.getOutgoingEdges(s).object2IntEntrySet()) {
                OutputLabel ol = trEntry.getKey();
                String label;
                label = (ol == null ? " " : ol.outputItems.toString() + "(" + ol.inputItem + ")");
                if(!ol.isEmpty())
                    fstVisualizer.add(String.valueOf(s), label, String.valueOf(trEntry.getIntValue()));
            }

            if (bz.isFinal(s))
                fstVisualizer.addFinalState(String.valueOf(s));
        }
        fstVisualizer.endGraph();
    }

    /** Export the still-relevant parts of this NFA to PDF */
    public void exportRelevantBdWithGraphViz(String file) {
        IntSet processedStates = new IntOpenHashSet();
        FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        fstVisualizer.beginGraph();

        graphStep(0, fstVisualizer, processedStates);
        fstVisualizer.endGraph();
    }

    /** Export one state, follow relevant paths recursively */
    private void graphStep(int s, FstVisualizer fstVisualizer, IntSet processedStates) {
        // export and follow all non-empty transitions
        processedStates.add(s);

        for (Object2IntMap.Entry<OutputLabel> trEntry : bz.getOutgoingEdges(s).object2IntEntrySet()) {
            OutputLabel ol = trEntry.getKey();
            String label;
            label = (ol == null ? " " : ol.outputItems.toString());
            if(!ol.isEmpty()) {
                fstVisualizer.add(String.valueOf(s), label, String.valueOf(trEntry.getIntValue()));
                if(!processedStates.contains(trEntry.getIntValue()))
                    graphStep(trEntry.getIntValue(), fstVisualizer, processedStates);
            }
        }

        if (bz.isFinal(s))
            fstVisualizer.addFinalState(String.valueOf(s));
    }


    /*
    IntSet allPivots = new IntOpenHashSet();
    public IntSet findPivots() {
        allPivots.clear();
        IntAVLTreeSet currentPivots = new IntAVLTreeSet();
        for(int pos = 0; pos<maxPos; pos++) {
            int s = checkForState(0, pos) ;
            if(s != -1) {
                findPivotsStep(s, currentPivots);
            }
        }
        return allPivots;
    }

    public void findPivotsStep(int s, IntAVLTreeSet currentPivots) {
//        int s = getOrCreateState(q, pos);
        if(isFinal.contains(s)) {
            allPivots.addAll(currentPivots);
        }
        for(Object2ObjectMap.Entry<OutputLabel, IntSet> out : forwardEdges.get(s).object2ObjectEntrySet()){
            IntAVLTreeSet followPivots;
            if(out.getKey() != null) {
                IntArrayList add = out.getKey().outputItems;
                followPivots = (IntAVLTreeSet) currentPivots.clone();
                // drop all items smaller than the smalles item in `add` from currentPivots
                while (followPivots.size() > 0 && followPivots.firstInt() < add.getInt(0)) {
                    followPivots.rem(followPivots.firstInt());
                }
                // now add all items
                for (int i = 0; i < add.size(); i++) {
                    if (followPivots.size() == 0 || add.getInt(i) > followPivots.firstInt()) {
                        followPivots.add(add.getInt(i));
                    }
                }

            } else {
                followPivots = currentPivots;
            }
            for (int sTo : out.getValue()) {
                findPivotsStep(sTo, followPivots);
            }

        }
    } */

}
