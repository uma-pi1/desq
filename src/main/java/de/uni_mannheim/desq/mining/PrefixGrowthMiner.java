package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.DesqProperties;
import it.unimi.dsi.fastutil.ints.*;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 * @author Rainer Gemulla (rgemulla@uni-mannheim.de)
 */
public final class PrefixGrowthMiner extends MemoryDesqMiner {
	// parameters for mining
    private long sigma;
    private int gamma;
    private int lambda;
    private boolean generalize = false;

	// helper variables
	private int beginItem = 0;
    private int endItem = Integer.MAX_VALUE;
    private int largestFrequentFid; // used to quickly determine whether an item is frequent
    private IntCollection ascendants; // used as a prefix for ascendant items
    final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator(); // used to access posting lists

    // if set to true, won't expand an items which have a parent that did not lead to a frequent expansion
    // there shouldn't be any reason to set this to false
    private static final boolean USE_PRUNING = true; // default: true

    // if set to true, posting lists are trimmed once computed. Saves up to 1/2 of memory for increased computational
    // cost
    // TODO: expose this as an option?
    static final boolean USE_TRIMMING = false; // default: false

    public PrefixGrowthMiner(DesqMinerContext ctx) {
		super(ctx);
		setParameters(ctx.conf);
	}

	// TODO: move up to DesqMiner?
	public void clear() {
		inputSequences.clear();
	}

    public static DesqProperties createConf(long sigma, int gamma, int lambda, boolean generalize) {
        DesqProperties conf = new DesqProperties();
        conf.setProperty("desq.mining.miner.class", PrefixGrowthMiner.class.getCanonicalName());
        conf.setProperty("desq.mining.min.support", sigma);
        conf.setProperty("desq.mining.max.gap", gamma);
        conf.setProperty("desq.mining.max.length", lambda);
        conf.setProperty("desq.mining.generalize", generalize);

        return conf;
    }

    public void setParameters(DesqProperties conf) {
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
        this.largestFrequentFid = ctx.dict.lastFidAbove(sigma);
        this.ascendants = ctx.dict.isForest() ? new IntArrayList() : new IntAVLTreeSet();
        clear();
	}

	// TODO: move to properties?
	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

    // TODO: move to properties?
	public void initialize(int b, int e) {
		clear();
		this.beginItem = b;
		this.endItem = e;
	}

	public void mine() {
        if (sumInputSupports >= sigma) {
            final PrefixGrowthTreeNode root = new PrefixGrowthTreeNode();

            // first runMiner through all data and create single-item posting lists
            for (int inputId=0; inputId<inputSequences.size(); inputId++) {
                final WeightedSequence inputSequence = inputSequences.get(inputId);
                for (int pos = 0; pos < inputSequence.size(); pos++) {
                    int itemFid = inputSequence.getInt(pos);
                    assert itemFid <= endItem;

                    // ignore gaps
                    if (itemFid < 0) {
                        continue;
                    }

                    // process item
                    if (largestFrequentFid >= itemFid) {
                        root.expandWithItem(itemFid, inputId, inputSequence.weight, pos);
                    }
                    if (generalize) {
                        ascendants.clear();
                        ctx.dict.addAscendantFids(itemFid, ascendants);
                        final IntIterator itemFidIt = ascendants.iterator();
                        while (itemFidIt.hasNext()) {
                            itemFid = itemFidIt.nextInt();
                            if (largestFrequentFid >= itemFid) {
                                root.expandWithItem(itemFid, inputId, inputSequence.weight, pos);
                            }
                        }
                    }
                }
            }

            // now the initial posting lists are constructed; traverse them
            root.expansionsToChildren(sigma);
            expand(new IntArrayList(), root, false);
        }
		clear();
	}

	// node must have been processed/output/expanded already, but children not
    // upon return, prefix must be unmodified
	private void expand(IntList prefix, PrefixGrowthTreeNode node, boolean hasPivot) {
        // add a placeholder to prefix
        final int lastPrefixIndex = prefix.size();
        prefix.add(-1);
        IntSet leftSiblingItemsWithoutFrequentChildNodes = new IntOpenHashSet();

        // iterate over children
        for (PrefixGrowthTreeNode childNode : node.children) {
            // output patterns (we know it's frequent by construction)
            assert childNode.support >= sigma;
            if (ctx.patternWriter != null) {
                prefix.set(lastPrefixIndex, childNode.itemFid);
                ctx.patternWriter.write(prefix, childNode.support);
            }

            // check if we need to expand
            boolean expand = prefix.size() < lambda;
            if (USE_PRUNING && expand) {
                ascendants.clear();
                ctx.dict.addAscendantFids(childNode.itemFid, ascendants);
                IntIterator itemFidIt = ascendants.iterator();
                while (itemFidIt.hasNext()) {
                    if (leftSiblingItemsWithoutFrequentChildNodes.contains(itemFidIt.next())) {
                        expand = false;
                        break;
                    }
                }
            }
            if (!expand) {
                childNode.invalidate();
                continue;
            }

            // ok, do the expansion
            int inputId = -1;
            projectedDatabaseIt.reset(childNode.projectedDatabase);
            do {
                inputId += projectedDatabaseIt.nextNonNegativeInt();
                final WeightedSequence inputSequence = inputSequences.get(inputId);

                // iterator over all positions
                int position = 0;
                while (projectedDatabaseIt.hasNext()) {
                    position += projectedDatabaseIt.nextNonNegativeInt();

                    // Add items in the right gamma+1 neighborhood
                    int gap = 0;
                    for (int newPosition = position+1; gap <= gamma && newPosition < inputSequence.size(); newPosition++) {
                        // process gaps
                        int itemFid = inputSequence.getInt(newPosition);
                        if (itemFid < 0) {
                            gap -= itemFid;
                            continue;
                        }
                        gap++;

                        // process item
                        if (largestFrequentFid >= itemFid) {
                            childNode.expandWithItem(itemFid, inputId, inputSequence.weight, newPosition);
                        }
                        if (generalize) {
                            ascendants.clear();
                            ctx.dict.addAscendantFids(itemFid, ascendants);
                            final IntIterator itemFidIt = ascendants.iterator();
                            while (itemFidIt.hasNext()) {
                                itemFid = itemFidIt.nextInt();
                                if(largestFrequentFid >= itemFid) {
                                    childNode.expandWithItem(itemFid, inputId, inputSequence.weight, newPosition);
                                }
                            }
                        }

                    }
                }
            } while (projectedDatabaseIt.nextPosting());

            // if this expansion did not produce any frequent children, then all siblings with descendant items
            // also can't produce frequent children; remember this item
            childNode.expansionsToChildren(sigma);
            if (USE_PRUNING && generalize && childNode.children.isEmpty()) {
                leftSiblingItemsWithoutFrequentChildNodes.add(childNode.itemFid);
            }

            // process just created expansions
            childNode.projectedDatabase = null; // not needed anymore
            final boolean containsPivot = hasPivot || (childNode.itemFid >= beginItem);
            expand(prefix, childNode, containsPivot);
            childNode.invalidate(); // not needed anymore
        }

        // remove placeholder from prefix
        prefix.removeInt(lastPrefixIndex);
	}
}
