package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * One state in a path through the Fst. We use this to build the NFAs we shuffle to the partitions
 *
 */
class PathState {
    protected final int id;
    protected int mergedInto;
    protected boolean isFinal = false;
    protected int level;
    protected OutputNFA nfa;
    protected int writtenNum = -1;


    /** Forward pointers */
    protected Object2ObjectSortedMap<OutputLabel,PathState> outTransitions;

    /** Backward pointer (in the tree, each state has only one incoming transition) */
    protected PathState predecessor;

    protected PathState(OutputNFA nfa, PathState from, int level) {
        this.nfa = nfa;
        id = nfa.numPathStates++;
        mergedInto = id;
        nfa.pathStates.add(this);
        outTransitions = new Object2ObjectAVLTreeMap<>();

        predecessor = from;
        this.level = level;

        nfa.leafs.add(this.id);
    }

    /**
     * Starting from this state, follow a given transition with given input item. Returns the state the transition
     * leads to.
     *
     * @param ol		the OutputLabel for this transition
     * @param nfa       the nfa the path is added to
     * @return
     */
    protected PathState followTransition(OutputLabel ol, OutputNFA nfa) {
//        counterFollowTransitionCalls++;

        // if we have a tree branch with this transition already, follow it and return the state it leads to
        if(outTransitions.containsKey(ol)) {
            // return this state
            PathState toState = outTransitions.get(ol);
            return toState;
        } else {
            // if we don't have a path starting with that transition, we create a new one
            //   (unless it is the last transition, in which case we want to transition to the global end state)
            PathState toState;
            // if we merge suffixes, create a state with backwards pointers, otherwise one without
            toState = new PathState(nfa, this, this.level+1);

            // add the transition and the created state to the outgoing transitions of this state and return the state the transition moves to
            outTransitions.put(ol.clone(), toState);

            if(outTransitions.size() == 1)
                nfa.leafs.remove(this.id);

//            counterTransitionsCreated++;
            return toState;
        }
    }

    /** Mark this state as final */
    public void setFinal() {
        this.isFinal = true;
    }

    /**
     * Determine whether this state is mergeable into the passed target state
     * @param target
     * @return
     */
    public boolean isMergeableInto(PathState target) {
//        counterIsMergeableIntoCalls++;

        if(this.isFinal != target.isFinal)
            return false;

        if(this.outTransitions.size() != target.outTransitions.size())
            return false;

        ObjectIterator<Object2ObjectMap.Entry<OutputLabel,PathState>> aIt = this.outTransitions.object2ObjectEntrySet().iterator();
        ObjectIterator<Object2ObjectMap.Entry<OutputLabel,PathState>> bIt = target.outTransitions.object2ObjectEntrySet().iterator();
        Object2ObjectMap.Entry<OutputLabel,PathState> a, b;

        // the two states have the same number of out transitions (checked above)
        // now we only check whether those out transitions have the same outKey and point towards the same state

        while(aIt.hasNext()) {
            a = aIt.next();
            b = bIt.next();

            if(!a.getKey().equals(b.getKey()))
                return false;

            if(a.getValue().mergedInto != b.getValue().mergedInto)
                return false;
        }

        return true;
    }


    /**
     * Serializes this state using by-path representation to the given IntList.
     * Processes un-processed children states recursively in DFS manner.
     *
     * @param send
     */
    protected void serializeDfs(IntList send) {
//        counterSerializedStates++;
        OutputLabel ol;
        PathState toState;

        // keep track of the number of serialized states
        nfa.numSerializedStates++; // we start state numbering at 1 for the serialization
        writtenNum = nfa.numSerializedStates;

        if(isFinal) {
            send.add(OutputNFA.FINAL);
            nfa.numSerializedFinalStates++;
        }

        boolean firstTransition = true;
        for(Object2ObjectMap.Entry<OutputLabel,PathState> entry : outTransitions.object2ObjectEntrySet()) {
//            counterSerializedTransitions++;
            ol = entry.getKey();
            toState = entry.getValue();

            // if this is the first transition we process at this state, we don't need to write and structural information.
            // we just follow the first path
            if(firstTransition) {
                firstTransition = false;
            } else {
                send.add(-(this.writtenNum+nfa.fst.numberDistinctItemEx()+1));
            }

            // serialize the output label
            if(ol.outputItems.size() > 1 && ol.outputItems.getInt(1) <= nfa.pivot) { // multiple output items, so we encode them using the transition
                // TODO: the current serialization format doesn't allow us to send two output items directly. We need to think about this.
                if(ol.outputItems.size() == 2 || ol.outputItems.getInt(2) > nfa.pivot) {
                    // we have only two output items, so we encode them directly
                    send.add(-1); // we use this as marker for two following output items
                    send.add(ol.outputItems.getInt(0));
                    send.add(ol.outputItems.getInt(1));
                } else {
                    send.add(-(nfa.fst.getItemExId(ol.tr)+1));
                    send.add(ol.inputItem); // TODO: we can generalize this for the pivot
                }
            } else { // there is only one (relevant) output item, and we know it's the first in the list
                send.add(ol.outputItems.getInt(0));
            }

            // serialize the to-state, either by processing it or by noting down it's number
            toState = nfa.getMergeTarget(toState.id);
            if(toState.writtenNum == -1) {
                toState.serializeDfs(send);
            } else {
                send.add(-(toState.writtenNum+nfa.fst.numberDistinctItemEx()+1));
            }
        }
    }
}

