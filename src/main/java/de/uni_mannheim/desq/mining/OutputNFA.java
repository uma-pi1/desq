package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.examples.DesqDfsRunDistributedMiningLocally;
import de.uni_mannheim.desq.examples.spark.DesqRunner;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.graphviz.FstVisualizer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.io.FilenameUtils;
import java.util.BitSet;

/**
 * A NFA that produces the output sequences of one input sequence.
 *
 * Per input sequence, we send one NFA to each partition, if the input sequence produces at least one
 * output sequence at that partition. Before sending the NFA, we trim it so we don't send paths that are
 * not relevant for that partition. We do the trimming while serializing.
 */
public class OutputNFA {
    /** The pivot item of this NFA */
    int pivot;

    /** A link to the FST we use to encode this NFA */
    Fst fst;

    /** Parallel lists to store the states of this NFA **/
    int numStates = 0;
    ObjectList<Object2IntAVLTreeMap<OutputLabel>> outgoingTransitions = new ObjectArrayList<>();
    BitSet isFinal = new BitSet();
    BitSet isLeaf =  new BitSet();
    IntArrayList mergedInto =  new IntArrayList();
    IntArrayList writtenNum =  new IntArrayList();
    IntArrayList predecessor = new IntArrayList();

    /** Counters for serialization */
    int numSerializedStates = 0;
    protected int numSerializedFinalStates = 0;

    /** Markers for last relevant and last irrelevant positions of the input sequence */
    int lastIrrelevant = Integer.MAX_VALUE;
    int lastRelevant = 0;

    /** If set to true, we have stopped to construct the NFA for this pivot.
     * Instead, we will send the relevant part of the input sequence. */
    boolean stoppedNFAconstruction = false;
    boolean useHybrid;


    /** Integer markers for NFA serialization */
    final static public int FINAL = 0;
    final static public int END_FINAL = Integer.MIN_VALUE;
    final static public int END = Integer.MAX_VALUE;

    /** Statistics */
    int numPaths = 0;


    public OutputNFA(int pivot, Fst fst, boolean useHybrid) {
        addState(-1); // create root state
        this.pivot = pivot;
        this.fst = fst;
        this.useHybrid = useHybrid;
    }

    /** If the given state was merged, returns the merge target. Otherwise, returns the original id. */
    public int getMergeTarget(int id) {
        if(id != mergedInto.getInt(id))
            return mergedInto.getInt(id);
        else
            return id;
    }

    /** Reset this NFA for reuse */
    public void clearAndPrepForPivot(int pivot) {
        numStates = 0;
        numSerializedStates = 0;
        numSerializedFinalStates = 0;
        numPaths = 0;
        isLeaf.clear();
        isFinal.clear();
        lastIrrelevant = Integer.MAX_VALUE;
        lastRelevant = 0;
        stoppedNFAconstruction = false;
        this.pivot = pivot;
        addState(-1);
    }

    /**
     * Add a path to this NFA.
     * Trims the outputItems sets of the OutputLabls in the path for the pivot of this NFA.
     * Returns the largest fid smaller than the pivot
     *
     * @param path
     */
    public int addPathAndReturnNextPivot(OutputLabel[] path, int currentLastIrrelevant, int pos) {
        numPaths++;
        int currentState;
        int nextLargest = -1, nextLargestInThisSet;
        boolean seenPivotAsSingle = false;
        OutputLabel ol;

        if(useHybrid) {
            // update the information about last irrelevant and last relevant (for this pivot) position of the input sequence
            lastIrrelevant = Math.min(lastIrrelevant, currentLastIrrelevant);
            lastRelevant = Math.max(lastRelevant, pos);

            // criterion for stopping to construct NFA (we stop if the NFA gets much larger than the relevant part of the input sequence)
            if (numStates > 5 * Math.abs(lastRelevant - lastIrrelevant)) {
                stoppedNFAconstruction = true;
            }
        }

        // Run through the transitions of this path and add them to this NFA
        currentState = 0; // 0 is always root
        for(int i=0; i<path.length; i++) {
            ol = path[i];

            // drop all output items larger than the pivot
            while(ol.outputItems.getInt(ol.outputItems.size()-1) > pivot) {
                ol.outputItems.removeInt(ol.outputItems.size()-1);
            }

            // find next largest item
            if(!seenPivotAsSingle) { // as soon as we know there won't be another round we don't need to do this anymore
                nextLargestInThisSet = ol.outputItems.getInt(ol.outputItems.size()-1);
                // if the largest item is the pivot, we get the next-largest (if there is one)
                if (nextLargestInThisSet == pivot) {
                    if (ol.outputItems.size() > 1) {
                        nextLargestInThisSet = ol.outputItems.getInt(ol.outputItems.size()-2);
                    } else {
                        // if this set has only the pivot item, it will be empty next run. so we note
                        //    that we need to stop after this run
                        seenPivotAsSingle = true;
                    }
                }
                nextLargest = Math.max(nextLargest, nextLargestInThisSet);
            }

            if(!stoppedNFAconstruction)
                currentState = followLabelFromState(ol, currentState);
        }

        // we ran through the path, so the current state is a final state.
        if(!stoppedNFAconstruction)
            isFinal.set(currentState);

        if(seenPivotAsSingle)
            return -1; // meaning, we have an empty set for any pivot smaller the current pivot, so we stop
        else
            return nextLargest;
    }

    /**
     * Starting from this state, follow a given transition with given input item. Returns the state the transition
     * leads to.
     *
     * @param state     starting state
     * @param ol		the OutputLabel for this transition
     * @return
     */
    protected int followLabelFromState(OutputLabel ol, int state) {
        Object2IntAVLTreeMap<OutputLabel> outTransitions = outgoingTransitions.get(state);

        // if we have a tree branch with this transition already, follow it and return the state it leads to
        if(outTransitions.containsKey(ol)) {
            // return this state
            int toState = outTransitions.getInt(ol);
            return toState;
        } else {
            // if we don't have a path starting with that transition, we create a new one
            int toState = addState(state);

            // add the transition and the created state to the outgoing transitions of this state and return the state the transition moves to
            outTransitions.put(ol.clone(), toState);

            if(outTransitions.size() == 1)
                isLeaf.clear(state);

            return toState;
        }
    }

    /** Adds a state to this NFA, with the given state as predecessor */
    private int addState(int fromState) {
        assert fromState < numStates : "The given fromState " + fromState + " is not a valid state (have " + numStates + ")";
        int id = numStates++;

        // do we need to create new objects or have we had this number of states before in this object?
        if(predecessor.size() > id) {
            // reset an old state
            outgoingTransitions.get(id).clear();
            mergedInto.set(id,id);
            writtenNum.set(id,-1);

            // set some info
            isLeaf.set(id);
            predecessor.set(id, fromState);
        } else {
            // create new ones
            outgoingTransitions.add(new Object2IntAVLTreeMap<>());
            mergedInto.add(id);
            writtenNum.add(-1);

            isLeaf.set(id);
            predecessor.add(fromState);
        }

        return id;
    }

    /** Merges the suffixes of this NFA */
    public void mergeSuffixes() {
        attemptMergeGroup(isLeaf, true);
    }

    /** Serializes this NFA without merging suffixes */
    public WeightedSequence serialize() {
        return mergeAndSerialize(false);
    }

    /** Merges the suffixes of this NFA and serialize it */
    public WeightedSequence mergeAndSerialize() {
        return mergeAndSerialize(true);
    }

    /** Serializes this NFA. Before that, it potentially merges the NFAs suffixes. Returns an integer list containing the serialized NFA. */
    public WeightedSequence mergeAndSerialize(boolean merge) {
        // merge suffixes
        if(merge)
            mergeSuffixes();

        // serialize
        WeightedSequence send = new WeightedSequence();
        numSerializedStates = 0;
        serializeStateUsingDfs(0, send); // 0 is always root

        // if we have only one final state and it's the last one, we don't need to send the marker
        if(numSerializedFinalStates == 1 && send.getInt(send.size()-1) == OutputNFA.FINAL) {
            send.size(send.size()-1);
        }

        return send;
    }

    public Sequence prepInputSequence(Sequence inputSequence) {
        return inputSequence.cloneSubListWithLeadingZero(lastIrrelevant, lastRelevant-1);
    }

    /**
     * Attempts to merge a group of states. That means, we try to merge subsets of the passed group of states. Each of those groups,
     * we process recursively.
     * @param stateIds
     */
    private void attemptMergeGroup(BitSet stateIds, boolean definitelyMergeable) {
        int target, merge;

        // we use this bitset to mark states that we have already merged in this iteration.
        // we use that to make sure we don't process them more than once
        BitSet alreadyMerged = new BitSet();

        // as we go over the passed set of states, we collect predecessor groups, which we will process afterwards
        BitSet predecessors = new BitSet();

        int i = 0, j;
        for (int currentTarget = stateIds.nextSetBit(0); currentTarget >= 0; currentTarget = stateIds.nextSetBit(currentTarget+1)) {
            // Retrieve the next state of the group. We will use this one as a merge target for all the following states
            // in the list
            i++;
            if(alreadyMerged.get(i)) // we don't process this state if we have merged it into another state already
                continue;
            target = getMergeTarget(currentTarget);

            // begin a new set of predecessors
            predecessors.clear();
            j = i;

            // Now compare this state to all the following states. If we find merge-compatible states, we merge them into this one
            for(int sId = stateIds.nextSetBit(currentTarget+1); sId >=0; sId = stateIds.nextSetBit(sId+1)) {
                j++;

                if(alreadyMerged.get(j))  // don't process if we already merged this state into another one
                    continue;

                merge = getMergeTarget(sId); // we retrieve the mapped number here. In case the state has already been merged somewhere else, we don't want to modify a merged state.

                if(merge == target) { // if the two states we are looking at are the same ones, we don't need to make any changes.
                    alreadyMerged.set(j);
                } else if(definitelyMergeable || isMergeableInto(merge, target)) { // check whether the two states are mergeable

                    // we write down that we merged this state into the target.
                    mergedInto.set(merge, target);

                    // also rewrite the original entry if the state we just merged was merged before
                    if(merge != sId)
                        mergedInto.set(merge, target);

                    // mark this state as merged, so we don't process it multiple times
                    alreadyMerged.set(j);

                    // if we have a group of predecessors (more than one), we process them as group
                    if(predecessor.getInt(target) != -1 && predecessor.getInt(merge) != -1) {
                        if (predecessor.getInt(target) != predecessor.getInt(merge)) {
                            predecessors.set(predecessor.getInt(merge));
                        }
                    }
                }
            }

            // if we have multiple predecessors, process them as group (later on)
            if(predecessors.cardinality()>0) {
                if(predecessor.getInt(target) != -1)
                    predecessors.set(predecessor.getInt(target));
                attemptMergeGroup(predecessors, false);
            }
        }
    }


    /**
     * Checks whether state merge is mergeable into state target.
     * Two states are mergeable if:
     *  (1) they are both final or both non-final, and
     *  (2) they have the same outgoing transitions (same labels, same to-states)
     *
     * @param merge
     * @param target
     * @return
     */
    public boolean isMergeableInto(int merge, int target) {

        if(isFinal.get(merge) != isFinal.get(target))
            return false;

        if(outgoingTransitions.get(merge).size() != outgoingTransitions.get(target).size())
            return false;

        ObjectIterator<Object2IntMap.Entry<OutputLabel>> aIt = outgoingTransitions.get(merge).object2IntEntrySet().iterator();
        ObjectIterator<Object2IntMap.Entry<OutputLabel>> bIt = outgoingTransitions.get(target).object2IntEntrySet().iterator();
        Object2IntMap.Entry<OutputLabel> a, b;

        // the two states have the same number of out transitions (checked above)
        // now we only check whether those out transitions have the same outKey and point towards the same state

        while(aIt.hasNext()) {
            a = aIt.next();
            b = bIt.next();

            if(!a.getKey().equals(b.getKey()))
                return false;

            if(mergedInto.getInt(a.getIntValue()) != mergedInto.getInt(b.getIntValue()))
                return false;
        }

        return true;
    }

    /**
     * Export the state of this NFA with their transitions to PDF
     * @param file
     */
    public void exportGraphViz(String file) {
        FstVisualizer fstVisualizer = new FstVisualizer(FilenameUtils.getExtension(file), FilenameUtils.getBaseName(file));
        fstVisualizer.beginGraph();
        for(int s = 0; s< numStates; s++) {
            if (mergedInto.getInt(s) == s) {
                for (Object2IntMap.Entry<OutputLabel> trEntry : outgoingTransitions.get(s).object2IntEntrySet()) {
                    OutputLabel ol = trEntry.getKey();
                    String label;
                    label = ol.outputItems.toString();
                    fstVisualizer.add(String.valueOf(s), label, String.valueOf(mergedInto.getInt(trEntry.getIntValue())));
                }
                if (isFinal.get(s))
                    fstVisualizer.addFinalState(String.valueOf(s));
            }
        }
        fstVisualizer.endGraph();
    }


    /**
     * Serializes the given state to a path-wise representation to the given IntList.
     * Processes un-processed successor states recursively in DFS manner.
     *
     * @param send
     */
    protected void serializeStateUsingDfs(int state, IntArrayList send) {
        OutputLabel ol;
        int toState;

        // keep track of the number of serialized states
        numSerializedStates++; // we start state numbering at 1 for the serialization
        writtenNum.set(state, numSerializedStates);

        if(isFinal.get(state)) {
            send.add(OutputNFA.FINAL);
            numSerializedFinalStates++;
        }

        boolean firstTransition = true;
        for(Object2IntMap.Entry<OutputLabel> entry : outgoingTransitions.get(state).object2IntEntrySet()) {
            ol = entry.getKey();
            toState = entry.getIntValue();

            // if this is the first transition we process at this state, we don't need to write and structural information.
            // we just follow the first path
            if(firstTransition) {
                firstTransition = false;
            } else {
                send.add(-(writtenNum.getInt(state)+fst.numberDistinctItemEx()+1));
            }

            // serialize the output label
            if(ol.outputItems.size() > 1 && ol.outputItems.getInt(1) <= pivot) { // multiple output items, so we encode them using the transition
                // TODO: the current serialization format doesn't allow us to send two output items directly. We need to think about this.
                if(ol.outputItems.size() == 2 || ol.outputItems.getInt(2) > pivot) {
                    // we have only two output items, so we encode them directly
                    send.add(-1); // we use this as marker for two following output items
                    send.add(ol.outputItems.getInt(0));
                    send.add(ol.outputItems.getInt(1));
                } else {
                    send.add(-(fst.getItemExId(ol.tr)+1));
                    send.add(ol.inputItem); // TODO: we can generalize this for the pivot
                }
            } else { // there is only one (relevant) output item, and we know it's the first in the list
                send.add(ol.outputItems.getInt(0));
            }

            // serialize the to-state, either by processing it or by noting down it's number
            toState = getMergeTarget(toState);
            if(writtenNum.getInt(toState) == -1) {
                serializeStateUsingDfs(toState, send);
            } else {
                send.add(-(writtenNum.getInt(toState)+fst.numberDistinctItemEx()+1));
            }
        }
    }


    public int pivot() {
        return pivot;
    }

    public boolean hasStoppedNFAconstruction() {
        return stoppedNFAconstruction;
    }
}

