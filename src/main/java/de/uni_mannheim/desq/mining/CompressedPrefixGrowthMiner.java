package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.ints.*;

import java.util.Properties;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 * @author Rainer Gemulla (rgemulla@uni-mannheim.de)
 */
public class CompressedPrefixGrowthMiner extends CompressedMemoryDesqMiner {
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
    private final IntSet ascendants = new IntOpenHashSet(); // used as a buffer for ascendant items
    NewPostingList.Iterator postingsIt = new NewPostingList.Iterator(); // used to access posting lists
    NewPostingList.Iterator inputIt;

	public CompressedPrefixGrowthMiner(DesqMinerContext ctx) {
		super(ctx);
        inputOffsets = new IntArrayList(); // we need those
		inputIt = inputSequences.iterator();
        setParameters(ctx.properties);
	}

	// TODO: move up to DesqMiner
	@Override
    public void clear() {
	    super.clear();
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
        if (inputSupports.size() >= sigma) {
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
                        ctx.dict.addAscendantFids(ctx.dict.getItemByFid(itemFid), ascendants);
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
            ProjectedDatabase projectedDatabase = childNode.projectedDatabase;
            assert projectedDatabase.support >= sigma;
            prefix.set(lastPrefixIndex, projectedDatabase.itemFid);
            if (ctx.patternWriter != null) {
                ctx.patternWriter.write(prefix, projectedDatabase.support);
            }

            // check if we need to expand
            if (prefix.size() >= lambda || childrenToNotExpand.contains(projectedDatabase.itemFid)) {
                childNode.clear();
                continue;
            }

            // ok, do the expansion
            postingsIt.reset(projectedDatabase.postingList);
            do {
                int inputId = postingsIt.nextNonNegativeInt();
                int inputOffset = inputOffsets.get(inputId);
                int inputSupport = inputSupports.get(inputId);

                // iterator over all positions
                while (postingsIt.hasNext()) {
                    inputIt.offset = inputOffset + postingsIt.nextNonNegativeInt();

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
                            ctx.dict.addAscendantFids(ctx.dict.getItemByFid(itemFid), ascendants);
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
            } while (postingsIt.nextPosting());

            // if this expansion did not produce any frequent children, then all siblings with descendant items
            // also can't produce frequent children; remember this
            childNode.expansionsToChildren(sigma);
            if (generalize && childNode.children.isEmpty()) {
                ctx.dict.addDescendantFids(ctx.dict.getItemByFid(projectedDatabase.itemFid), childrenToNotExpand);
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
