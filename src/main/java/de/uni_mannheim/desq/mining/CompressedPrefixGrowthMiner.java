package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 * @author Rainer Gemulla (rgemulla@uni-mannheim.de)
 */
@Deprecated // should be integrated via an improved posting list to avoid code duplicaton
public class CompressedPrefixGrowthMiner extends CompressedMemoryDesqMiner {
	// parameters for mining
    private long sigma;
    private int gamma;
    private int lambda;
    private boolean generalize = false;

	// helper variables
	private int beginItem = 0;
    private int endItem = Integer.MAX_VALUE;
    private int largestFrequentFid; // used to quickly determine whether an item is frequent
    private final IntSet ascendants = new IntAVLTreeSet(); // used as a prefix for ascendant items
    final PostingList.Iterator projectedDatabaseIt = new PostingList.Iterator(); // used to access posting lists
    final PostingList.Iterator inputIt = inputSequences.iterator();

	public CompressedPrefixGrowthMiner(DesqMinerContext ctx) {
		super(ctx);
        inputOffsets = new IntArrayList(); // we need those
        setParameters(ctx.conf);
	}

	// TODO: move up to DesqMiner
	@Override
    public void clear() {
	    super.clear();
	}

    public static Configuration createConf(long sigma, int gamma, int lambda, boolean generalize) {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setThrowExceptionOnMissing(true);
        conf.setProperty("desq.mining.miner.class", CompressedPrefixGrowthMiner.class.getCanonicalName());
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
        if (inputSupports.size() >= sigma) {
            final PrefixGrowthTreeNode root = new PrefixGrowthTreeNode();

            for (int inputId = 0; inputId < inputSupports.size(); inputId++) {
                int inputOffset = inputOffsets.get(inputId);
                inputIt.offset = inputOffset;
                int inputSupport = inputSupports.get(inputId);
                while (inputIt.hasNext()) { // iterator over items in the sequence
                    int itemFid = inputIt.nextInt();
                    assert itemFid <= endItem;

                    // ignore gaps
                    if (itemFid < 0) {
                        continue;
                    }

                    // process item
                    int nextItemOffset = inputIt.offset-inputOffset;
                    if (largestFrequentFid >= itemFid) {
                        root.expandWithItem(itemFid, inputId, inputSupport, nextItemOffset);
                    }
                    if (generalize) {
                        ascendants.clear();
                        ctx.dict.addAscendantFids(itemFid, ascendants);
                        IntIterator itemFidIt = ascendants.iterator();
                        while (itemFidIt.hasNext()) {
                            itemFid = itemFidIt.nextInt();
                            if (largestFrequentFid >= itemFid) {
                                root.expandWithItem(itemFid, inputId, inputSupport, nextItemOffset);
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
        int lastPrefixIndex = prefix.size();
        prefix.add(-1);
        IntSet childrenToNotExpand = new IntOpenHashSet();

        // iterate over children
        for (PrefixGrowthTreeNode childNode : node.children) {
            // output patterns (we know it's frequent by construction)
            assert childNode.support >= sigma;
            prefix.set(lastPrefixIndex, childNode.itemFid);
            if (ctx.patternWriter != null) {
                ctx.patternWriter.write(prefix, childNode.support);
            }

            // check if we need to expand
            if (prefix.size() >= lambda || childrenToNotExpand.contains(childNode.itemFid)) {
                childNode.clear();
                continue;
            }

            // ok, do the expansion
            int inputId = -1;
            projectedDatabaseIt.reset(childNode.projectedDatabase);
            do {
                inputId += projectedDatabaseIt.nextNonNegativeInt();
                int inputOffset = inputOffsets.get(inputId);
                int inputSupport = inputSupports.get(inputId);

                // iterator over all positions
                int position = 0;
                while (projectedDatabaseIt.hasNext()) {
                    position += projectedDatabaseIt.nextNonNegativeInt();
                    inputIt.offset = inputOffset + position;

                    // Add items in the right gamma+1 neighborhood
                    int gap = 0;
                    while (gap <= gamma && inputIt.hasNext()) {
                        // process gaps
                        int itemFid = inputIt.nextInt();
                        if (itemFid < 0) {
                            gap -= itemFid;
                            continue;
                        }
                        gap++;

                        // process item
                        int nextItemOffset = inputIt.offset-inputOffset;
                        if (largestFrequentFid >= itemFid) {
                            childNode.expandWithItem(itemFid, inputId, inputSupport, nextItemOffset);
                        }
                        if (generalize) {
                            ascendants.clear();
                            ctx.dict.addAscendantFids(itemFid, ascendants);
                            IntIterator itemFidIt = ascendants.iterator();
                            while (itemFidIt.hasNext()) {
                                itemFid = itemFidIt.nextInt();
                                if(largestFrequentFid >= itemFid) {
                                    childNode.expandWithItem(itemFid, inputId, inputSupport, nextItemOffset);
                                }
                            }
                        }

                    }
                }
            } while (projectedDatabaseIt.nextPosting());

            // if this expansion did not produce any frequent children, then all siblings with descendant items
            // also can't produce frequent children; remember this
            childNode.expansionsToChildren(sigma);
            if (generalize && childNode.children.isEmpty()) {
                ctx.dict.addDescendantFids(ctx.dict.getItemByFid(childNode.itemFid), childrenToNotExpand);
            }

            // process just created expansions
            childNode.projectedDatabase.clear(); // not needed anymore
            boolean containsPivot = hasPivot || (childNode.itemFid >= beginItem);
            expand(prefix, childNode, containsPivot);
            childNode.clear(); // not needed anymore
        }

        // remove placeholder from prefix
        prefix.removeInt(lastPrefixIndex);
	}
}
