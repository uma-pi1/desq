package de.uni_mannheim.desq.mining;


import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.util.IntArrayStrategy;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public final class CSpadeMiner extends DesqMiner {

	// -- parameters --------------------------------------------------------------------------------

	/** Minimum support */
	protected long sigma;

	/** Maximum gap */
	protected int gamma;

	/** Maximum length */
	protected int lambda;

    protected boolean generalize;

	/**
	 * Start of pivot range (see class description). Set to 0 to mine all frequent
	 * sequences.
	 */
	protected int beginItem = 0;

	/**
	 * End of pivot range (see class description). Set to <code>Integer.MAX_VALUE</code>
	 * to mine all frequent sequences.
	 */
	protected int endItem = Integer.MAX_VALUE;

	// -- internal variables ------------------------------------------------------------------------

	// At any point of time, we store an inverted index that maps subsequences of
	// length k to their posting lists.
	//
	// During data input, we have k=2. Every input sequence is added to the
	// index (by generating all its (2,gamma)-subsequences and then discarded.
	//
	// During frequent sequence de.uni_mannheim.desq.old.mining, we compute repeatedly compute length-(k+1)
	// subsequences from the length-k subsequences.

	/** Length of subsequences currently being mined */
	protected int k;

	/**
	 * A list of sequences of length k; no sequence occurs more than once. Each
	 * sequence is stored in either uncompressed or compressed format.
	 * 
	 * In uncompressed format, each sequence is encoded as an array of exactly k
	 * item identifiers. When k=2, all sequences are stored in uncompressed
	 * format.
	 * 
	 * In compressed format, each sequence is encoded as a length-2 array (p, w).
	 * To reconstruct the uncompressed sequence, take the first k-1 items from the
	 * sequence at position p in kSequences (p is a "prefix pointer") and set the
	 * k-th item to w (suffix item). When k>2, an entry is compressed when it has
	 * two elements and uncompressed when it has k elements.
	 */
	protected ArrayList<int[]> kSequences = new ArrayList<>();

	/**
	 * Maps 2-sequence to their index entries. Only used during data input, k=2.
	 */
    final Map<int[], KPatternIndexEntry> twoSequenceIndex = new Object2ObjectOpenCustomHashMap<>(
			new IntArrayStrategy());

	/** Holds information about a posting list. Used only during data input, k=32. */
	protected class KPatternIndexEntry {
		int index;
		int lastInputId;
		int lastPosition;
	}

	/** Used as a temporary prefix during data input. */
	protected final int[] twoSequence = new int[2];

	/**
	 * Posting list for each sequence in kSequences.
	 */
	protected ArrayList<PostingList> kPostingLists = new ArrayList<>();

	/**
	 * Support of each input sequence (indexed by inputId). If an input
	 * sequence has support larger than one, is it treated as if it had occured
	 * in the data as many times as given by its support value.
	 */
	protected final IntList inputSupports = new IntArrayList();

	/**
	 * Total support for each sequence in kSequences. Identical to the sum of the
	 * supports of each input sequence that occurs in the posting list.
	 */
	protected IntList kTotalSupports = new IntArrayList();

    /** Stores frequency of individual items (only needed when generalize = false) */
    protected final Int2IntMap itemDFreqs = new Int2IntOpenHashMap();

    /** IntSets for temporary use */
    final IntSet itemFids = new IntAVLTreeSet();
    final IntSet ascendantFids = new IntAVLTreeSet();
    final IntSet otherAscendantFids = new IntAVLTreeSet();

    private int largestFrequentFid; // used to quickly determine whether an item is frequent

    public CSpadeMiner(DesqMinerContext ctx) {
		super(ctx);
        setParameters(ctx.conf);
	}

	/** Flushes all internal data structures. */
	public void clear() {
		k = 2;
        itemDFreqs.clear();
		twoSequenceIndex.clear();
		kSequences.clear();
		kPostingLists.clear();
		kTotalSupports.clear();
		inputSupports.clear();
	}

	public static Configuration createConf(long sigma, int gamma, int lambda, boolean generalize) {
		PropertiesConfiguration conf = new PropertiesConfiguration();
		conf.setThrowExceptionOnMissing(true);
		conf.setProperty("desq.mining.miner.class", CSpadeMiner.class.getCanonicalName());
		conf.setProperty("desq.mining.min.support", sigma);
		conf.setProperty("desq.mining.max.gap", gamma);
		conf.setProperty("desq.mining.max.length", lambda);
		conf.setProperty("desq.mining.generalize", generalize);

		return conf;
	}

	public void setParameters(Configuration conf) {
		long sigma = conf.getLong("desq.mining.min.support");
		int gamma = conf.getInt("desq.mining.max.gap");
		int lambda = conf.getInt("desq.mining.max.length");
		boolean generalize = conf.getBoolean("desq.mining.generalize");
		setParameters(sigma, gamma, lambda, generalize);
	}

    public void setParameters(long sigma, int gamma, int lambda, boolean generalize) {
        this.sigma = sigma;
        this.gamma = gamma;
        this.lambda = lambda;
        this.generalize = generalize;
        this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
        clear();
    }

	/**
	 * Initialize frequent sequence miner (without pivots). Should be called
	 * before any data input.
	 */
	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

	/**
	 * Initialize frequent sequence miner (with pivots). Should be called before
	 * any data input.
	 * 
	 * @param beginItem
	 *          begin of pivot range (see class description)
	 * @param endItem
	 *          end of pivot range (see class description)
	 */
	public void initialize(int beginItem, int endItem) {
		clear();
		this.beginItem = beginItem;
		this.endItem = endItem;
	}

	// -- input phase -------------------------------------------------------------------------------

	@Override
    public void addInputSequence(IntList inputSequence) {
	    addInputSequence(inputSequence, 1);
    }

	public void addInputSequence(IntList inputSequence, int inputSupport) {
		// only valid during data input phase
		assert k <= 2;
		assert kSequences.size() == twoSequenceIndex.size();

		// store the support of the input input sequence
		int inputId = inputSupports.size();
		inputSupports.add(inputSupport);

		// Add the input sequence to the inverted index. Here we construct all
		// gapped 2-sequences and update their corresponding posting lists
		int position = 0; // current position in expanded sequence (i.e., without compressed gaps)
        itemFids.clear();
		for (int pos = 0; pos < inputSequence.size(); pos++) {
			int itemFid = inputSequence.getInt(pos);
		    assert itemFid <= endItem; // contract of this class

			// skip gaps
			if (itemFid < 0) {
				position -= itemFid;
				continue;
			}

			// skip infrequent items
			if (!generalize && itemFid > largestFrequentFid) {
			    position++;
                continue;
            }

			// count individual item frequencies unless we are generalizing
			if (!generalize) {
                itemFids.add(itemFid);
            }

            // no need to do more if we only need 1-items
            if (lambda < 2) continue;

			// create all 2-subsequences
			// pos points to first item, otherPos to second item
			for (int otherPos = pos + 1, gap=0; gap<=gamma && otherPos < inputSequence.size(); otherPos++) {
                int otherItemFid = inputSequence.get(otherPos);

				// skip gaps
				if (otherItemFid < 0) {
				    gap -= otherItemFid;
					continue;
				}
                gap++;

                // skip infrequent items
                if (!generalize && otherItemFid > largestFrequentFid) {
                    continue;
                }

				// we found a valid 2-sequence; create a posting for the two sequence
				// and its generalizations
                if (!generalize) {
                    twoSequence[0] = itemFid;
                    twoSequence[1] = otherItemFid;
                    addPosting(twoSequence, inputId, inputSupport, position);
                } else {
                    // generate all ascendants
                    ascendantFids.clear();
                    ctx.dict.addAscendantFids(itemFid, ascendantFids);
                    ascendantFids.add(itemFid);
                    otherAscendantFids.clear();
                    ctx.dict.addAscendantFids(otherItemFid, otherAscendantFids);
                    otherAscendantFids.add(otherItemFid);

                    // generate all pairs of frequent items
                    IntIterator fidIt = ascendantFids.iterator();
                    while (fidIt.hasNext()) {
                        int fid = fidIt.nextInt();
                        if (fid > largestFrequentFid)
                            continue;
                        twoSequence[0] = fid;
                        IntIterator otherFidIt = otherAscendantFids.iterator();
                        while (otherFidIt.hasNext()) {
                            int otherFid = otherFidIt.nextInt();
                            if (otherFid > largestFrequentFid) {
                                otherFidIt.remove();
                                continue;
                            }
                            twoSequence[1] = otherFid;
                            addPosting(twoSequence, inputId, inputSupport, position);
                        }
                    }
                }
            }
			position++;
		}

		// update 1-item counts
		for (int itemFid : itemFids) {
            int count = inputSupport;
            if (itemDFreqs.containsKey(itemFid)) {
                count += itemDFreqs.get(itemFid);
            }
            itemDFreqs.put(itemFid, count);
        }
	}

	/**
	 * Adds an occurrence of a 2-sequence to the inverted index. Only used for
	 * 2-sequences during the input phase. The provided kSequence is not stored,
	 * i.e., can be reused.
	 */
	protected void addPosting(int[] kSequence, int inputId, int inputSupport, int position) {
		// get the posting list for the current sequence
		// if the sequence has not seen before, create a new posting list
		KPatternIndexEntry entry = twoSequenceIndex.get(kSequence);

		PostingList postingList;

		if (entry == null) {
			// we never saw this 2-sequence before
			entry = new KPatternIndexEntry();
			entry.index = kSequences.size();
			entry.lastInputId = -1;
			kSequence = new int[] { kSequence[0], kSequence[1] }; // copy necessary here
			kSequences.add(kSequence);
			twoSequenceIndex.put(kSequence, entry);
			postingList = new PostingList();
			kPostingLists.add(postingList);
			kTotalSupports.add(0);
		} else {
			// a new occurrence of a previously seen 2-sequence
			postingList = kPostingLists.get(entry.index);
		}

		// add the current occurrence to the posting list
		if (entry.lastInputId != inputId) {
			postingList.newPosting();

			// add input sequence id
			postingList.addNonNegativeInt(inputId);
			postingList.addNonNegativeInt(position);

			// update data structures
			entry.lastInputId = inputId;
			entry.lastPosition = position;
			kTotalSupports.set(entry.index, kTotalSupports.get(entry.index) + inputSupport);
		} else if (entry.lastPosition != position) { // don't add any position more than once
			postingList.addNonNegativeInt(position);
			entry.lastPosition = position;
		}
	}

	/**
	 * Finalizes the input phase by computing the overall support of each
	 * 2-sequence and pruning all 2-sequences below minimum support.
	 */
	public void computeTwoPatterns() {
		// returning the 2-sequences that have support equal or above minsup and
		// their posting lists
		twoSequenceIndex.clear(); // not needed anymore
		// does not exist anymore: inputSupports.trim(); // will not be changed anymore

		// compute total support of each sequence and remove sequences with support
		// less than sigma
		for (int id = 0; id < kSequences.size();) {
			if (kTotalSupports.get(id) >= sigma) {
				// accept sequence
				// uncomment next line to save some space during 1st phase (but: increased runtime)
				// postingList.trimToSize();
				id++; // next id
			} else {
				// delete the current sequence (by moving the last sequence to the current position)
				int size1 = kPostingLists.size() - 1;
				if (id < size1) {
					kSequences.set(id, kSequences.remove(size1));
					kPostingLists.set(id, kPostingLists.remove(size1));
					kTotalSupports.set(id, kTotalSupports.get(size1));
                    kTotalSupports.remove(size1);
				} else {
					kSequences.remove(size1);
					kPostingLists.remove(size1);
					kTotalSupports.remove(size1);
				}
				// same id again (now holding a different kSequence)
			}
		}
	}

	// -- de.uni_mannheim.desq.old.mining phase ------------------------------------------------------------------------------

    @Override
	public void mine() {
        // output 1-patterns
        if (ctx.patternWriter != null && lambda >= 1) {
            IntList itemFids = new IntArrayList();
            itemFids.add(-1); // place holder
            if (!generalize) {
                for (Int2IntMap.Entry entry : itemDFreqs.int2IntEntrySet()) {
                    int dFreq = entry.getIntValue();
                    if (dFreq >= sigma) {
                        itemFids.set(0, entry.getIntKey());
                        ctx.patternWriter.write(itemFids, entry.getIntValue());
                    }
                }
            } else {
                for (Item item : ctx.dict.getItems()) {
                    if (item.dFreq >= sigma) {
                        itemFids.set(0, item.fid);
                        ctx.patternWriter.write(itemFids, item.dFreq);
                    }
                }
            }
        }

        // compute and output all other patterns
        if (lambda >= 2) {
            computeTwoPatterns();
            outputKPatterns();
            while ((k < lambda) && !kSequences.isEmpty()) {
                bfsTraversal();
                outputKPatterns();
            }
        }

		clear();
	}

	/**
	 * Outputs all k-sequences that contain a pivot.
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void outputKPatterns() {
		int[] prefixSequence = null;
		int[] temp = new int[k];

		// walk over all sequences
		for (int i = 0; i < kSequences.size(); i++) {
			int[] sequence = kSequences.get(i);

			// uncompress sequence (if necessary)
			if (k == 2 || sequence.length == k) {
				// uncompressed sequence
				prefixSequence = sequence;
			} else {
				// compressed sequence (entries = (prefix pointer, suffix item)
				// reconstruct whole sequence by taking first k-1 symbols taken from
				// previous sequence plus the given suffix
				System.arraycopy(prefixSequence, 0, temp, 0, prefixSequence.length - 1);
				temp[k - 1] = sequence[1]; // suffix item
				sequence = temp;
			}

			// check if the sequence contains a pivot
			boolean hasPivot = false;
			for (int word : sequence) {
				if (word >= beginItem) {
					assert word <= endItem; // contract of this class
					hasPivot = true;
					break;
				}
			}

			// if contains a pivot, output the sequence and its support
			if (hasPivot && ctx.patternWriter != null) {
				ctx.patternWriter.write(sequence, kTotalSupports.get(i));
			}

		}
	}

	/**
	 * This method constructs all frequent (k+1)-sequences from the set of
	 * k-sequences (values of k, kSequences, kPostings, kTotalSupport are
	 * updated).
	 */
	protected void bfsTraversal() {
		// terminology (example from 5- to 6-sequences)
		// k : 5
		// k1 : 6
		// k- sequence : abcde
		// prefix : abcd (= right join key)
		// suffix : bcde (= left join key)

		// build prefix/suffix indexes (maps prefix/suffix to list of sequences with
		// this prefix/suffix) values point to indexes in kSequences
		Map<IntArrayList, IntArrayList> sequencesWithSuffix = new Object2ObjectOpenHashMap<>();
		Map<IntArrayList, IntArrayList> sequencesWithPrefix = new Object2ObjectOpenHashMap<>();
		buildPrefixSuffixIndex(sequencesWithPrefix, sequencesWithSuffix);

		// variables for the (k+1)-sequences
		int k1 = k + 1;
		ArrayList<int[]> k1Sequences = new ArrayList<>();
		ArrayList<PostingList> k1PostingLists = new ArrayList<>();
		IntArrayList k1TotalSupports = new IntArrayList();

		// temporary variables
		PostingList postingList = new PostingList(); // list of postings for a new (k+1) sequence
		PostingList.Iterator leftPostingList = new PostingList.Iterator(); // posting list of the left k-sequence
		PostingList.Iterator rightPostingList = new PostingList.Iterator(); // posting list of the right k-sequence

		// we now join sequences (e.g., abcde) that end with some suffix with
		// sequences
		// that start with the same prefix (e.g., bcdef)
		for (Map.Entry<IntArrayList, IntArrayList> entry : sequencesWithSuffix.entrySet()) {
			// if there is no right key to join, continue
			IntArrayList joinKey = entry.getKey();
			IntArrayList rightSequences = sequencesWithPrefix.get(joinKey); // indexes of right sequences
			if (rightSequences == null) {
				continue;
			}

			// there are right keys for the join, so let's join
			IntArrayList leftSequences = entry.getValue(); // indexes of left sequences
			for (int i = 0; i < leftSequences.size(); i++) {
				// get the postings of that sequence for joining
				leftPostingList.reset( kPostingLists.get(leftSequences.getInt(i)) );

				// compression
				// total number of successful joins for the current left sequence
				int noK1SequencesForLeftSequence = 0;
				int pointerToFirstK1Sequence = -1; // index of first join match

				// for every right key that matches the current left key, perform
				// a merge join of the posting lists (match if we find two postings
				// of the same input sequence such that the starting position of the right
				// sequence is close enough to the starting position of the left
				// sequence (at most gamma items in between)
				for (int j = 0; j < rightSequences.size(); j++) {
					// initialize
					postingList.clear();
					int totalSupport = 0; // of the current posting list
					leftPostingList.offset = 0;
					rightPostingList.reset( kPostingLists.get(rightSequences.getInt(j)) );
					rightPostingList.offset = 0;
					int leftInputId = leftPostingList.nextNonNegativeInt();
					int rightInputId = rightPostingList.nextNonNegativeInt();
					boolean foundMatchWithLeftInputId = false;

					while (leftPostingList.hasNext() && rightPostingList.hasNext()) {
						// invariant: leftPostingList and rightPostingList point to first
						// position after a input sequence id

						if (leftInputId == rightInputId) {
							// potential match; now check offsets
							int inputId = leftInputId;
							int rightPosition = -1;
							while (leftPostingList.hasNext()) {
								int leftPosition = leftPostingList.nextNonNegativeInt();

								// fast forward right cursor (merge join; positions are sorted)
								while (rightPosition <= leftPosition && rightPostingList.hasNext()) {
									rightPosition = rightPostingList.nextNonNegativeInt();
								}
								if (rightPosition <= leftPosition)
									break;

								// check whether join condition is met
								if (rightPosition <= leftPosition + gamma + 1) {
									// yes, add a posting
									if (!foundMatchWithLeftInputId) {
									    postingList.newPosting();
										postingList.addNonNegativeInt(inputId);
										foundMatchWithLeftInputId = true;
										totalSupport += inputSupports.get(inputId);
									}
									postingList.addNonNegativeInt(leftPosition);
								}
							}

							// advance both join lists
							if (rightPostingList.nextPosting()) {
								rightInputId = rightPostingList.nextNonNegativeInt();
							}
							if (leftPostingList.nextPosting()) {
								leftInputId = leftPostingList.nextNonNegativeInt();
								foundMatchWithLeftInputId = false;
							}
							// end leftInputId == rightTransactionId
						} else if (leftInputId > rightInputId) {
							// advance right join list (merge join; lists sorted by
							// input sequence id)
							if (rightPostingList.nextPosting()) {
								rightInputId = rightPostingList.nextNonNegativeInt();
							}
						} else {
							// advance left join (merge join; lists sorted by input sequence id)
							if (leftPostingList.nextPosting()) {
								leftInputId = leftPostingList.nextNonNegativeInt();
								foundMatchWithLeftInputId = false;
							}
						}
					}

					// if the new (k+1)-sequence has support equal or above minimum support,
					// add it to the result of this round
					if (totalSupport >= sigma) {
						noK1SequencesForLeftSequence++;
						int suffixItem = this.kSequences.get(rightSequences.getInt(j))[this.kSequences.get(rightSequences.getInt(j)).length - 1];
						int[] kSequence; // holds result

						if (noK1SequencesForLeftSequence == 1) {
							// uncompressed output
							pointerToFirstK1Sequence = k1Sequences.size();

							// construct whole (k+1)-sequence
							kSequence = new int[k1];
							int[] prefix = kSequences.get(leftSequences.getInt(i));
							if (prefix.length == k1 - 1 || k1 <= 3) { // prefix sequence is uncompressed
								System.arraycopy(prefix, 0, kSequence, 0, prefix.length);
							} else { // prefix sequence is compressed (only suffix item stored)
								// need to retrieve prefix from initial sequence
								int prefixPos = prefix[0];
								int[] tempPrefix = kSequences.get(prefixPos);
								System.arraycopy(tempPrefix, 0, kSequence, 0, tempPrefix.length - 1);
								kSequence[k1 - 2] = prefix[1];
							}
							kSequence[k1 - 1] = suffixItem;
						} else {
							// include only the suffix item of (k+1)-sequence (first k items
							// same as the ones at index pointerToPrefixSequence)
							kSequence = new int[2];
							kSequence[0] = pointerToFirstK1Sequence;
							kSequence[1] = suffixItem;
						}

						// store in results of current round
						k1Sequences.add(kSequence);
						k1PostingLists.add(new PostingList(postingList)); // copying necessary here; postingList reused
						k1TotalSupports.add(totalSupport);
					}
				} // for all right sequences of the same key
			} // for all left sequences of each left key
		} // for all left keys

		// we are done; store output
		k = k1;
		this.kSequences = k1Sequences;
		this.kPostingLists = k1PostingLists;
		this.kTotalSupports = k1TotalSupports;
	}

	/** Builds a prefix/suffix index from the currently stored k-sequences */
	void buildPrefixSuffixIndex(Map<IntArrayList, IntArrayList> sequencesWithPrefix,
			Map<IntArrayList, IntArrayList> sequencesWithSuffix) {
		int k1 = k + 1;

		// scan over all k-sequences and build prefix/suffix index
		IntArrayList suffix = new IntArrayList();
		IntArrayList prefix = new IntArrayList();
		for (int index = 0; index < kSequences.size(); index++) {
			int[] sequence = kSequences.get(index);

			// construct prefix (last item of sequence omitted) and suffix (first item omitted)
			if (sequence.length == 2 && k1 > 3) {
				// compressed sequence
				// only suffix in sequence, need to construct left key
                suffix.clear();
                for (int j = 1; j < prefix.size(); j++) {
					suffix.add(prefix.getInt(j));
				}
				suffix.add(sequence[1]);
				// right key remains unchanged
			} else {
				// uncompressed sequence
				prefix.clear();
				for (int j = 0; j < k - 1; j++) {
					prefix.add(sequence[j]);
				}

				suffix.clear();
				for (int j = 1; j < k; j++) {
					suffix.add(sequence[j]);
				}
			}

			// update list of sequences starting with the prefix
			IntArrayList sequences = sequencesWithPrefix.get(prefix);
			if (sequences == null) {
				sequences = new IntArrayList();
				sequencesWithPrefix.put(new IntArrayList(prefix), sequences);
			}
			sequences.add(index);

			// update list of sequences ending with suffix
			sequences = sequencesWithSuffix.get(suffix);
			if (sequences == null) {
				sequences = new IntArrayList();
				sequencesWithSuffix.put(new IntArrayList(suffix), sequences);
			}
			sequences.add(index);
		}
	}

}
