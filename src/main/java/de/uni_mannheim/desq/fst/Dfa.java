package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.util.IntSetUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2ShortMap;
import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap;

import java.util.*;


/** A DFA corresponding (and linked to) an {@link Fst}. */
public final class Dfa {
	/** The initial state. */
	protected DfaState initial;

	/** Maps a set of FST states (as given in the bitset) to a DfaState of this DFA, if present. */
	protected Map<BitSet, DfaState> states = new HashMap<BitSet, DfaState>();

	/** Creates a DFA for the given FST. The DFA accepts each input for which the FST has an accepting run
	 * with all output items <= largestFrequentItemFid. */
	public static Dfa createDfa(Fst fst, Dictionary dict, int largestFrequentItemFid,
								boolean processFinalCompleteStates) {
		return create(fst, dict, largestFrequentItemFid, false, processFinalCompleteStates);
	}

	/** Creates a reverse DFA for the given FST and modifies the FST for efficient use in Desq's two-pass
	 * algorithms. The DFA accepts each reversed input for which the (unmodified) FST has an accepting
	 * run with all output items <= largestFrequentItemFid. Note that the modified FST should not be used
	 * directly anymore, but only in conjunction with {@link #acceptsReverse(IntList, List, IntList)}. */
	public static Dfa createReverseDfa(Fst fst, Dictionary dict, int largestFrequentItemFid,
									   boolean processFinalCompleteStates) {
		return create(fst, dict, largestFrequentItemFid, true, processFinalCompleteStates);
	}

	private static Dfa create(Fst fst, Dictionary dict, int largestFrequentItemFid, boolean reverse,
							  boolean processFinalCompleteStates) {
		// compute the initial states
		BitSet initialStateIdSet = new BitSet(fst.numStates());
		if(reverse) { // create a DFA for the reverse FST (original FST is destroyed)
			fst.dropAnnotations();
			List<State> initialStates = fst.reverse(false);
			for(State s : initialStates) {
				initialStateIdSet.set(s.id);
			}
			fst.annotate();
			fst.dropCompleteFinalTransitions();
		} else { // don't reverse, original FST remains unmodified
			initialStateIdSet.set(fst.getInitialState().getId());
		}

		// construct the DFA
		Dfa dfa = new Dfa();
		dfa.initial = new EagerDfaState(initialStateIdSet, fst);
		dfa.constructEager(initialStateIdSet, fst, dict, largestFrequentItemFid, processFinalCompleteStates);

		// when we are reversing, reverse back the FST to get an optimized new FST
		if (reverse) {
			fst.dropAnnotations();
			fst.reverse(false);
			fst.annotate();
		}

		return dfa;
	}

	/** Construct an extended DFA from the given FST */
	private void constructEager(BitSet initialStateIdSet, Fst fst, Dictionary dict, int largestFrequentItemFid,
								boolean processFinalCompleteStates) {
		states.clear();

		// GLOBAL DATA STRUCTURES
		// unprocessed dfa states
		Stack<BitSet> unprocessedToStates = new Stack<>();

		// map from transition label (e.g., "(.^)") to items that fire (used as cache)
		Map<String, IntList> firedItemsCache = new HashMap<>();

		// map from set of transition labels to a DFA state (used to avoid duplicate computations)
		// whenever two DFA states have the same set of outgoing transition labels (ignoring where they go),
		// then we reuse EagerDfaState#transitions and just rewire the target states appropriately
		Map<String, Int2ShortMap> transitionsCache = new HashMap<>();

		// map from set of transition labels to information to where the target state is stored in
		// EagerDfaState#toStates for each (active) combination of those transitions
		Map<String, Object2ShortMap<BitSet>> positionsCache = new HashMap<>();

		// DATA STRUCTURES FOR CURRENTLY PROCESSED DFA STATE
		// fst states reachable by all items
		// used to represent all outgoing FST transitions where Transition#firesAll is true
		BitSet defaultTransition = new BitSet(fst.numStates());

		// map from transition label to reachable FST states (excluding fires-all transitions)
		SortedMap<String, BitSet> toStatesMap = new TreeMap<>();

		// for all items, which of the outgoing transitions fire (excluding fires-all transitions)
		BitSet activeFids = new BitSet(dict.lastFid()+1); // indexed by item
		BitSet[] firedTransitionsOf = new BitSet[dict.lastFid()+1]; // indexed by item
		for (int i=0; i<firedTransitionsOf.length; i++) {
			firedTransitionsOf[i] = new BitSet();
		}


		// MAIN LOOP: while there is an unprocessed state, compute all its transitions
		// starting with the initial state
		states.put(initialStateIdSet, initial);
		unprocessedToStates.push(initialStateIdSet);
		while (!unprocessedToStates.isEmpty()) {
			// get next state to process
			BitSet fromStates = unprocessedToStates.pop();
			EagerDfaState fromDfaState = (EagerDfaState) states.get(fromStates);

			//System.out.println("Processing " + fromStates.toString());

			// if the state is final complete and the option to not process those state is set, we do compute
			// the outgoing transitions of this state
			if (!processFinalCompleteStates && fromDfaState.isFinalComplete()) {
				continue;
			}

			// iterate over all relevant transitions and compute reachable FST states per transition label encountered
			// if we see a label that we haven't seen before, we also compute the set of items that fire the transition
			defaultTransition.clear(); // computed now
			toStatesMap.clear(); // computed now
			for (int stateId = fromStates.nextSetBit(0); // iterate over states
				 stateId >= 0;
				 stateId = fromStates.nextSetBit(stateId + 1)) {
				// ignore outgoing transitions from final complete states
				State state = fst.getState(stateId);
				if (state.isFinalComplete())
					continue;

				for (Transition t : state.getTransitions()) { // iterate over transitions
					if (t.firesAll(largestFrequentItemFid)) {
						// this is an optmization which often helps when the pattern expression contains .
						defaultTransition.set(t.getToState().getId());
					} else {
						// otherwise we remember the transition
						String label = t.toPatternExpression();
						BitSet toStates = toStatesMap.computeIfAbsent(label, k -> new BitSet(fst.numStates()));
						toStates.set(t.getToState().getId());

						// if it was a new label, compute the fired items
						if (!firedItemsCache.containsKey(label)) {
							IntArrayList firedItems = new IntArrayList(dict.lastFid() + 1);
							IntIterator it = t.matchedFidIterator();
							while (it.hasNext()) {
								int fid = it.nextInt();
								boolean matches = !t.hasOutput() || t.matchesWithFrequentOutput(fid, largestFrequentItemFid);
								if (matches) {
									firedItems.add(fid);
								}
							}
							firedItems.trim();
							firedItemsCache.put(label, firedItems);
							// System.out.println(label + " fires for " + firedItems.size() + " items");
						}
					}
				}
			}

			// now set the default transition in case there were transitions that fire on all items
			if (!defaultTransition.isEmpty()) {
				EagerDfaState toDfaState = (EagerDfaState) states.get(defaultTransition);
				if (toDfaState == null) {
					BitSet toStates = IntSetUtils.copyOf(defaultTransition);
					toDfaState = new EagerDfaState(toStates, fst);
					states.put(toStates, toDfaState);
					unprocessedToStates.add(toStates);
				}
				fromDfaState.defaultTransition = toDfaState;
			}

			// if there were no other transitions, we are done
			if (toStatesMap.isEmpty())
				continue; // no non-default transitions

			// get all of the remaining transitions into arrays (label and for each label, set of to-states)
			String[] labelArray = toStatesMap.keySet().toArray(new String[] {}); // sorted (since sorted map)
			BitSet[] toStatesArray = toStatesMap.values().toArray(new BitSet[] {}); // sorted conformingly
			String key = String.join(" ", labelArray);

			// add the transitions to the DFA state
			if (!transitionsCache.containsKey(key)) {
				// we haven't seen this combination of transitions -> compute everything from scratch

				// we first compute which transitions fire per item
				activeFids.clear();
				for (int t = 0; t < labelArray.length; t++) { // iterate over transitions
					IntList firedItems = firedItemsCache.get(labelArray[t]);
					for (int i = 0; i < firedItems.size(); i++) {
						int fid = firedItems.get(i);
						if (!activeFids.get(fid)) {
							// activate and initialize fid if not yet seen
							activeFids.set(fid);
							firedTransitionsOf[fid].clear();
							firedTransitionsOf[fid].set(t);
						}

						// add the states we can reach with this fid
						firedTransitionsOf[fid].set(t);
					}
				}

				// now iterate over the items and add transitions to the DFA
				fromDfaState.transitions = new Int2ShortOpenHashMap(activeFids.cardinality());
				Object2ShortMap<BitSet> positionMap = new Object2ShortOpenHashMap<>();
				for (int fid = activeFids.nextSetBit(0);
					 fid >= 0;
					 fid = activeFids.nextSetBit(fid + 1)) {

					// get the position of the corresponding next state in EagerDfaState#toStates
					BitSet firedTransitions = firedTransitionsOf[fid];
					short toStatesPos = positionMap.getShort(firedTransitions);
					if (toStatesPos == 0) { // not present
						// compute subsequent state
						BitSet toStates = new BitSet();
						toStates.or(defaultTransition); // always fires
						for (int t = firedTransitions.nextSetBit(0);
							 t >= 0;
							 t = firedTransitions.nextSetBit(t + 1)) {
							toStates.or(toStatesArray[t]);
						}

						// get the corresponding FST state
						EagerDfaState toDfaState = (EagerDfaState) states.get(toStates);
						if (toDfaState == null) {
							toDfaState = new EagerDfaState(toStates, fst);
							states.put(toStates, toDfaState);
							unprocessedToStates.add(toStates);
						}

						// add the state as a successor state to the DFA
						fromDfaState.toStates.add(toDfaState);
						if (fromDfaState.toStates.size() > Short.MAX_VALUE)
							throw new IllegalStateException("Only up to 32767 to-states supported");
						toStatesPos = (short) (fromDfaState.toStates.size() - 1);
						positionMap.put(IntSetUtils.copyOf(firedTransitions), toStatesPos);
					}

					// add the transition
					fromDfaState.transitions.put(fid, toStatesPos);
				}

				// cache
				transitionsCache.put(key, fromDfaState.transitions);
				positionsCache.put(key, positionMap);
			} else {
				// reuse transition index from a previously processed state with the same outgoing FST transisions
				fromDfaState.transitions = transitionsCache.get(key);
				Object2ShortMap<BitSet> positionMap = positionsCache.get(key);
				fromDfaState.toStates.addAll(Collections.nCopies(positionMap.size(), null)); // resize to correct size

				// iterate over active combinations of fired transitions
				for (Object2ShortMap.Entry<BitSet> entry : positionMap.object2ShortEntrySet()) {
					BitSet firedTransitions = entry.getKey();
					short toStatesPos = entry.getShortValue();

					// compute subsequent state for this combination of fired transitions
					BitSet toStates = new BitSet();
					toStates.or(defaultTransition); // always fires
					for (int t = firedTransitions.nextSetBit(0);
						 t >= 0;
						 t = firedTransitions.nextSetBit(t + 1)) {
						toStates.or(toStatesArray[t]);
					}

					// get the corresponding FST state
					EagerDfaState toDfaState = (EagerDfaState) states.get(toStates);
					if (toDfaState == null) {
						toDfaState = new EagerDfaState(toStates, fst);
						states.put(toStates, toDfaState);
						unprocessedToStates.add(toStates);
					}

					// and put it to the corresponding positon
					fromDfaState.toStates.set(toStatesPos, toDfaState);
				}
			}
		}
	}

	/** Returns true if the input sequence is relevant (DFA accepts). */
	public boolean accepts(IntList inputSequence) {
		DfaState state = initial;
		int pos = 0;
		while(pos < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(pos++));
			// In this case is ok to return false, if there was a final state before
			// we already retured true, final state can not be reached if state
			// was null
			if(state == null)
				return false;
			if(state.isFinalComplete())
				return true;
		}
		return state.isFinal(); // last state; pos == inputSequence.size()
	}

	/** Returns true if the reverse input sequence is relevant (DFA accepts reverse input).
	 *
	 * The method reads the input sequence backwards and records the sequence of states being traversed.
	 *
	 * @param inputSequence the input sequence
	 * @param stateSeq sequence of DFA states being traversed on the reversed sequence, i.e.,
	 *                    <code>stateSeq[inputSequence.size() - (pos+1)] = state before consuming inputSequence[pos]</code>
	 * @param initialPos positions from which the FST (modified by {@link #createReverseDfa(Fst, Dictionary, int, boolean)}) needs
	 *                   to be started to find all accepting runs.
	 */
	public boolean acceptsReverse(IntList inputSequence, List<DfaState> stateSeq, IntList initialPos) {
		DfaState state = initial;
		stateSeq.add(state);
		int pos = inputSequence.size();
		while(pos > 0) {
			state = state.consume(inputSequence.getInt(--pos));
			if(state == null)
				break; // we may return true or false, as we might have reached a final state before
			stateSeq.add(state);
			if(state.isFinalComplete() || (state.isFinal() && pos == 0)) {
				initialPos.add(pos);
			}
		}
		return (!initialPos.isEmpty());
	}

	public int numStates() {
		return states.size();
	}
}
