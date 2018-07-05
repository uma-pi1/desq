package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import de.uni_mannheim.desq.util.ReverseDFA;
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

import java.util.Arrays;
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
public class QPGrid {
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

    /** Stores the initial states (we use this for trimming input sequences) */
    IntOpenHashSet initialState = new IntOpenHashSet();

    /** Stores the minimum and maximum relevant position for each pivot item of the input sequence */
    private Int2LongOpenHashMap pivotMinMax = new Int2LongOpenHashMap();

    /** Maps from internal state id (i.e. s) to external state (i.e. q). Only initialized if needed because of trimAdvanced. */
    private Int2IntOpenHashMap qByS = null;

    /** Stores the minimum output item at each position of the input sequence. */
    int[] minimumOutputItemAtPosition = null;

    /** Stores potentially irrelevant positions (as BitSet) of the input sequence. */
    BitSet potentiallyIrrelevantPositions = null;

    /** Stores the minimum relevant position for a state s of the grid */
    int[] minPos;

    /** We cache our output labels, so that we can use identical objects if one OutputLabel occurs multiple times  */
    Object2ObjectOpenHashMap<IntArrayList,OutputLabel> outputLabelCache = new Object2ObjectOpenHashMap<>();

    /** Maximum FST state q and position, for which we have a state */
    int maxPos = 0;
    int maxQ = 0;

    /** Data structures for determinizing the NFA */
    ReverseDFA reverseDfa = new ReverseDFA(this);

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
        initialState.clear();
        pivotMinMax.clear();
        qByS = null;
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
        // we do not add this edge if:
        // - the edge does not produce output,
        // - the target state does not produce any further output, and
        // - the current state is already final
        // (this prevents unnecessary eps-edges towards the end of the grid)
        if(label == null && hasNoFollowingOutput(qTo, pos+1) && isFinal(qFrom, pos)) {
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

            // mark this state as initial
            if(q == 0)
                initialState.add(s);
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
    public boolean hasAcceptingPaths() {
        return sByQp.size() > 0;
    }

    /** Returns the minimum and maximum relevant position for a given pivot item */
    public long minMaxForPivot(int pivot) {
        return pivotMinMax.get(pivot);
    }

    /** Returns the set of pivot items for this grid*/
    public IntSet getPivotsForward(boolean trim, boolean trimAdvanced) {
        IntOpenHashSet pivots = new IntOpenHashSet();

        ObjectArrayList<IntAVLTreeSet> potentialPivots = new ObjectArrayList<>();

        // clear the minimum pivot positions
        if(trim) {
            if (minPos == null || minPos.length < numStates()) {
                minPos = new int[numStates()];
            }
            Arrays.fill(minPos, Integer.MAX_VALUE);
        }

        boolean output = false;

        if (trimAdvanced) {
            // construct an inverted index as we need to find state changes later on
            qByS = new Int2IntOpenHashMap();
            for(Long2IntOpenHashMap.Entry entry : sByQp.long2IntEntrySet()) {
                qByS.put(entry.getIntValue(), PrimitiveUtils.getLeft(entry.getLongKey()));
            }

            if (minimumOutputItemAtPosition == null || minimumOutputItemAtPosition.length < maxPos) {
                minimumOutputItemAtPosition = new int[maxPos];
            }
            Arrays.fill(minimumOutputItemAtPosition, Integer.MAX_VALUE);

            if (potentiallyIrrelevantPositions == null || potentiallyIrrelevantPositions.length() < maxPos) {
                potentiallyIrrelevantPositions = new BitSet(maxPos);
            }
            potentiallyIrrelevantPositions.set(0, maxPos + 1);
        }

        for(int s=0; s<=numStates(); s++) {
            potentialPivots.add(new IntAVLTreeSet());
        }

        for(int pos = 0; pos<=maxPos; pos++) {
            for(int q = 0; q<=maxQ; q++) {
                // we assume that states are potentially irrelevant by default
                boolean thisStateIsPotentiallyIrrelevant = true;

                if(isDeadEnd(q, pos)) {
                    // TODO (actually never reached for vldb-example?)
                    System.out.println("TODO");
                }

                int s = checkForState(q, pos);
                if(output) System.out.println(q + "," + pos + ": s=" + s);

                if(s != -1) {
                    // an existing state must first quality as potentially irrelevant
                    thisStateIsPotentiallyIrrelevant = false;

                    if(isFinal(q,pos)) {
                        if (output) System.out.println("is final. adding " + potentialPivots.get(s));
                        pivots.addAll(potentialPivots.get(s));

                        // update minimum and maximum relevant positions for these pivot items
                        if (trim) {
                            for (int pivot : potentialPivots.get(s)) {
                                long minMax = pivotMinMax.getOrDefault(pivot, -1L);
                                if (minMax == -1L) {
                                    pivotMinMax.put(pivot, PrimitiveUtils.combine(minPos[s], pos));
                                } else {
                                    pivotMinMax.put(pivot, PrimitiveUtils.combine(Math.min(PrimitiveUtils.getLeft(minMax), minPos[s]), Math.max(PrimitiveUtils.getRight(minMax), pos)));
                                }
                            }
                        }
                    }

                    if(output) System.out.println("Has potential pivots: " + potentialPivots.get(s));
                    for(Object2ObjectMap.Entry<OutputLabel, IntSet> edge : forwardEdges.get(s).object2ObjectEntrySet()) {
                        OutputLabel ol = edge.getKey();
                        int trimMin = Math.max(ol == null ? 0 : ol.outputItems.getInt(0), potentialPivots.get(s).size() > 0 ? potentialPivots.get(s).firstInt() : 0);
                        for(int sTo : edge.getValue()) {
                            if (output)
                                System.out.println("has outgoing edge " + ol + " (trim=" + trimMin + ") to state " + sTo);
                            if (ol != null)
                                for (int item : ol.outputItems)
                                    if (item >= trimMin) {
                                        potentialPivots.get(sTo).add(item);
                                        if (output) System.out.println("Adding " + item);
                                    }
                            if (potentialPivots.get(s).size() > 0)
                                for (int item : potentialPivots.get(s))
                                    if (item >= trimMin) {
                                        potentialPivots.get(sTo).add(item);
                                        if (output) System.out.println("Adding " + item);
                                    }

                            // propagate the minimum position to the to-state
                            if (trim) {
                                // usually, we propagate the state's minPos forward
                                int propPos = minPos[s];

                                // if this state has no relevant item before it (minPos = MAX) and this item
                                // is relevant (i.e., creates output or leaves the initial state), we propagate this pos as min
                                if (minPos[s] == Integer.MAX_VALUE && ((ol != null && ol.outputItems.size() > 0) || !initialState.contains(sTo))) {
                                    propPos = pos;
                                }

                                // propagate: toState.minPos = min(toState.minPos, propPos)
                                if (propPos < minPos[sTo])
                                    minPos[sTo] = propPos;
                            }

                            if (trimAdvanced) {
                                if (ol == null || ol.outputItems.isEmpty()) {
                                    if (q == qByS.get(sTo)) {
                                        // this is an epsilon-transition that does not change state: the current state
                                        // qualifies itself again as potentially irrelevant
                                        thisStateIsPotentiallyIrrelevant = true;
                                    } else {
                                        // this is an epsilon-transition that changes state: the current position must
                                        // be relevant
                                        potentiallyIrrelevantPositions.set(pos, false);
                                    }
                                } else {
                                    // we observe a transition with output: update the minimum output item of this
                                    // position
                                    minimumOutputItemAtPosition[pos] = Math.min(minimumOutputItemAtPosition[pos],
                                            ol.outputItems.getInt(0));
                                }
                            }
                        }
                    }
                }

                if(trimAdvanced) {
                    // all states of a position must be potentially irrelevant to have a potentially
                    // irrelevant position
                    potentiallyIrrelevantPositions.set(pos,
                            potentiallyIrrelevantPositions.get(pos) && thisStateIsPotentiallyIrrelevant);
                }
            }
        }

        return pivots;
    }

    /** Extracts the pivot NFAs from this grid. Assumes that the grid was constructed already. */
    public void constructPivotNFAs(ObjectArrayList<Sequence> serializedNFAs, Fst fst) {
        // we first determinize the grid back->front
        reverseAndDeterminize();

        // PDF exports
//        exportGraphViz(seq + "-grid.pdf"); // export the grid
//        exportBdWithGraphViz(seq + "-reverse-determinized.pdf"); // export the reverse determinized NFA

        // then we construct the per-pivot NFAs
        reverseDfa.constructNFAs(serializedNFAs, fst);
    }


    /** Determinizes the NFA backwards into <code>this.reverseDfa</code> */
    public void reverseAndDeterminize() {
        reverseDfa.clear();

        IntSet finalStates = new IntOpenHashSet();
        for(long l : isFinal) {
            int s = checkForState(l);
            if(s != -1)
                finalStates.add(s);
        }
        int currentState = reverseDfa.addNewState(finalStates);

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
            outgoingEdges = collectOutgoingEdges(reverseDfa.getIncludedOriginalStates(currentState));

            // add outgoing edges to the current state
            for(Object2ObjectMap.Entry<OutputLabel, IntSet> outgoingEdge : outgoingEdges.object2ObjectEntrySet()) {
                int toState = reverseDfa.getOrCreateState(outgoingEdge.getValue());
                reverseDfa.addEdge(currentState, outgoingEdge.getKey(), toState);
            }
            currentState++;
        } while (currentState < reverseDfa.numStates());
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
        System.out.println("q, p -> s -- maxPiv");
        for(Long2IntOpenHashMap.Entry entry : sByQp.long2IntEntrySet()) {
            System.out.println(PrimitiveUtils.getLeft(entry.getLongKey()) + ", " + PrimitiveUtils.getRight(entry.getLongKey()) + " -> " + entry.getIntValue() + " -- " + maxPivot.getInt(entry.getIntValue()));
        }

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
        for (int s = 0; s < reverseDfa.numStates(); s++) {
            for (Object2IntMap.Entry<OutputLabel> trEntry : reverseDfa.getOutgoingEdges(s).object2IntEntrySet()) {
                OutputLabel ol = trEntry.getKey();
                String label;
                label = (ol == null ? " " : ol.outputItems.toString() + "(" + ol.inputItem + ")");
                if(!ol.isEmpty())
                    fstVisualizer.add(String.valueOf(s), label, String.valueOf(trEntry.getIntValue()));
            }

            if (reverseDfa.isFinal(s))
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

        for (Object2IntMap.Entry<OutputLabel> trEntry : reverseDfa.getOutgoingEdges(s).object2IntEntrySet()) {
            OutputLabel ol = trEntry.getKey();
            String label;
            label = (ol == null ? " " : ol.outputItems.toString());
            if(!ol.isEmpty()) {
                fstVisualizer.add(String.valueOf(s), label, String.valueOf(trEntry.getIntValue()));
                if(!processedStates.contains(trEntry.getIntValue()))
                    graphStep(trEntry.getIntValue(), fstVisualizer, processedStates);
            }
        }

        if (reverseDfa.isFinal(s))
            fstVisualizer.addFinalState(String.valueOf(s));
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
