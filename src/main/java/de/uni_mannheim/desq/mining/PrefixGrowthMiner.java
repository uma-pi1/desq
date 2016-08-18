package de.uni_mannheim.desq.mining;

import java.util.Properties;

import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.*;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 * @author Rainer Gemulla (rgemulla@uni-mannheim.de)
 */
public class PrefixGrowthMiner extends MemoryDesqMiner {
	// parameters for mining
    private long sigma;
    private int gamma;
    private int lambda;
    private boolean generalize = false;

	// helper variables
	private int beginItem = 0;
    private int endItem = Integer.MAX_VALUE;
    private final PrefixGrowthTreeNode root = new PrefixGrowthTreeNode(new ProjectedDatabase());
    private int largestFrequentFid; // used to quickly determine whether an item is frequent
    private IntCollection ascendants; // used as a buffer for ascendant items
    final PostingList.Iterator postingsIt = new PostingList.Iterator(); // used to access posting lists

    // if set to true, won't expand an items which have a parent that did not lead to a frequent expansion
    // there shouldn't be any reason to set this to false
    private static final boolean USE_PRUNING = true; // default: true

    // if set to true, posting lists are trimmed once computed. Saves up to 1/2 of memory for increased computational
    // cost
    // TODO: expose this as an option?
    static final boolean USE_TRIMMING = false; // default: false

    public PrefixGrowthMiner(DesqMinerContext ctx) {
		super(ctx);
		setParameters(ctx.properties);
	}

	// TODO: move up to DesqMiner
	public void clear() {
		inputSequences.clear();
		inputSupports.clear();
		root.clear();
	}

	public static Properties createProperties(long sigma, int gamma, int lambda, boolean generalize) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "minSupport", sigma);
		PropertiesUtils.set(properties, "maxGap", gamma);
		PropertiesUtils.set(properties, "maxLength", lambda);
		PropertiesUtils.set(properties, "generalize", generalize);
		return properties;
	}

	public void setParameters(Properties properties) {
		long sigma = PropertiesUtils.getLong(properties, "minSupport");
		int gamma = PropertiesUtils.getInt(properties, "maxGap");
		int lambda = PropertiesUtils.getInt(properties, "maxLength");
		boolean generalize = PropertiesUtils.getBoolean(properties, "generalize");
        setParameters(sigma, gamma, lambda, generalize);
	}

	public void setParameters(long sigma, int gamma, int lambda, boolean generalize) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.generalize = generalize;
        this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
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
        if (inputSequences.size() >= sigma) {
            // first run through all data and create single-item posting lists
            for (int inputId=0; inputId<inputSequences.size(); inputId++) {
                final int[] inputSequence = inputSequences.get(inputId);
                final int inputSupport = inputSupports.get(inputId);
                for (int pos = 0; pos < inputSequence.length; pos++) {
                    int itemFid = inputSequence[pos];
                    assert itemFid <= endItem;

                    // ignore gaps
                    if (itemFid < 0) {
                        continue;
                    }

                    // process item
                    if (largestFrequentFid >= itemFid) {
                        root.expandWithItem(itemFid, inputId, inputSupport, pos);
                    }
                    if (generalize) {
                        ascendants.clear();
                        ctx.dict.addAscendantFids(itemFid, ascendants);
                        final IntIterator itemFidIt = ascendants.iterator();
                        while (itemFidIt.hasNext()) {
                            itemFid = itemFidIt.nextInt();
                            if (largestFrequentFid >= itemFid) {
                                root.expandWithItem(itemFid, inputId, inputSupport, pos);
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
            final ProjectedDatabase projectedDatabase = childNode.projectedDatabase;
            assert projectedDatabase.support >= sigma;
            if (ctx.patternWriter != null) {
                prefix.set(lastPrefixIndex, projectedDatabase.itemFid);
                ctx.patternWriter.write(prefix, projectedDatabase.support);
            }

            // check if we need to expand
            boolean expand = prefix.size() < lambda;
            if (USE_PRUNING && expand) {
                ascendants.clear();
                ctx.dict.addAscendantFids(projectedDatabase.itemFid, ascendants);
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
            postingsIt.reset(projectedDatabase.postingList);
            do {
                final int inputId = postingsIt.nextNonNegativeInt();
                final int[] inputSequence = inputSequences.get(inputId);
                final int inputSupport = inputSupports.get(inputId);

                // iterator over all positions
                while (postingsIt.hasNext()) {
                    final int position = postingsIt.nextNonNegativeInt();

                    // Add items in the right gamma+1 neighborhood
                    int gap = 0;
                    for (int newPosition = position+1; gap <= gamma && newPosition < inputSequence.length; newPosition++) {
                        // process gaps
                        int itemFid = inputSequence[newPosition];
                        if (itemFid < 0) {
                            gap -= itemFid;
                            continue;
                        }
                        gap++;

                        // process item
                        if (largestFrequentFid >= itemFid) {
                            childNode.expandWithItem(itemFid, inputId, inputSupport, newPosition);
                        }
                        if (generalize) {
                            ascendants.clear();
                            ctx.dict.addAscendantFids(itemFid, ascendants);
                            final IntIterator itemFidIt = ascendants.iterator();
                            while (itemFidIt.hasNext()) {
                                itemFid = itemFidIt.nextInt();
                                if(largestFrequentFid >= itemFid) {
                                    childNode.expandWithItem(itemFid, inputId, inputSupport, newPosition);
                                }
                            }
                        }

                    }
                }
            } while (postingsIt.nextPosting());

            // if this expansion did not produce any frequent children, then all siblings with descendant items
            // also can't produce frequent children; remember this item
            childNode.expansionsToChildren(sigma);
            if (USE_PRUNING && generalize && childNode.children.isEmpty()) {
                leftSiblingItemsWithoutFrequentChildNodes.add(projectedDatabase.itemFid);
            }

            // process just created expansions
            childNode.projectedDatabase = null; // not needed anymore
            final boolean containsPivot = hasPivot || (projectedDatabase.itemFid >= beginItem);
            expand(prefix, childNode, containsPivot);
            childNode.invalidate(); // not needed anymore
        }

        // remove placeholder from prefix
        prefix.removeInt(lastPrefixIndex);
	}
}
