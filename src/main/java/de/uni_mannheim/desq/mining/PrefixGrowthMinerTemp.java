package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.*;

import java.util.Properties;

/** Temporary shortcut implementaiton to investigate performace.
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 * @author Rainer Gemulla (rgemulla@uni-mannheim.de)
 */
public class PrefixGrowthMinerTemp extends MemoryDesqMiner {
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
    private final IntSet ascendants = new IntAVLTreeSet(); // used as a buffer for ascendant items
    final PostingList.Iterator postingsIt = new PostingList.Iterator(); // used to access posting lists

	public PrefixGrowthMinerTemp(DesqMinerContext ctx) {
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
                int[] inputSequence = inputSequences.get(inputId);
                int inputSupport = inputSupports.get(inputId);
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
                        while (true) {
                            int from = ctx.dict.parentFidsOffsets[itemFid];
                            int to = ctx.dict.parentFidsOffsets[itemFid+1];
                            if (from == to) break;
                            itemFid = ctx.dict.parentFids[from];
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
        int lastPrefixIndex = prefix.size();
        prefix.add(-1);
        IntSet itemsWithoutFrequentChildren = new IntOpenHashSet();

        // iterate over children
        for (PrefixGrowthTreeNode childNode : node.children) {
            // output patterns (we know it's frequent by construction)
            ProjectedDatabase projectedDatabase = childNode.projectedDatabase;
            assert projectedDatabase.support >= sigma;
            prefix.set(lastPrefixIndex, projectedDatabase.itemFid);
            if (ctx.patternWriter != null) {
                ctx.patternWriter.write(prefix, projectedDatabase.support);
            }

            // check if we need to expand
            boolean expand = prefix.size() < lambda;
            if (expand) {
                ascendants.clear();
                ctx.dict.addAscendantFids(projectedDatabase.itemFid, ascendants);
                ascendants.retainAll(itemsWithoutFrequentChildren);
                expand = ascendants.isEmpty();
            }
            if (!expand) {
                childNode.clear();
                continue;
            }

            // ok, do the expansion
            int inputId = -1;
            postingsIt.reset(projectedDatabase.postingList);
            do {
                inputId += postingsIt.nextNonNegativeInt();
                int[] inputSequence = inputSequences.get(inputId);
                int inputSupport = inputSupports.get(inputId);

                // iterator over all positions
                int position = 0;
                while (postingsIt.hasNext()) {
                    position += postingsIt.nextNonNegativeInt();

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
                            while (true) {
                                int from = ctx.dict.parentFidsOffsets[itemFid];
                                int to = ctx.dict.parentFidsOffsets[itemFid+1];
                                if (from == to) break;
                                itemFid = ctx.dict.parentFids[from];
                                if (largestFrequentFid >= itemFid) {
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
            if (generalize && childNode.children.isEmpty()) {
                itemsWithoutFrequentChildren.add(projectedDatabase.itemFid);
            }

            // process just created expansions
            childNode.projectedDatabase.clear(); // not needed anymore
            boolean containsPivot = hasPivot || (projectedDatabase.itemFid >= beginItem);
            expand(prefix, childNode, containsPivot);
            childNode.clear(); // not needed anymore
        }

        // remove placeholder from prefix
        prefix.removeInt(lastPrefixIndex);
    }
}
