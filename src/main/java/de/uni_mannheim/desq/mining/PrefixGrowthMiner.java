package de.uni_mannheim.desq.mining;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import de.uni_mannheim.desq.util.PropertiesUtils;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.*;
import mining.PostingList;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 *
 * Also needs to be cleaned up.
 */
public class PrefixGrowthMiner extends DesqMiner {
	// parameters for mining
	protected long sigma;
	protected int gamma;
	protected int lambda;
	protected boolean generalize = false;

	// helper variables
	protected ArrayList<int[]> inputTransactions = new ArrayList<>();
	protected IntArrayList transactionSupports = new IntArrayList();
	private int noOutputPatterns = 0;
	protected int beginItem = 0;
	protected int endItem = Integer.MAX_VALUE;
	protected Items globalItems = new Items();
	private int[] transaction = null;
	public int itemsCounted = 0;
	int largestFrequentFid;
	IntSet ascendants = new IntOpenHashSet();

	public PrefixGrowthMiner(DesqMinerContext ctx) {
		super(ctx);
		setParameters(ctx.properties);
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
	}

	public void clear() {
		inputTransactions.clear();
		transactionSupports.clear();
		globalItems.clear();
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
		long sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		int gamma = PropertiesUtils.getInt(ctx.properties, "maxGap");
		int lambda = PropertiesUtils.getInt(ctx.properties, "maxLength");
		boolean generalize = PropertiesUtils.getBoolean(ctx.properties, "generalize");
        setParameters(sigma, gamma, lambda, generalize);
	}

	public void setParameters(long sigma, int gamma, int lambda, boolean generalize) {
        clear();
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.generalize = generalize;
        clear();
	}
	
	
	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

	public void initialize(int b, int e) {
		clear();
		this.beginItem = b;
		this.endItem = e;
	}

	@Override
	public void addInputSequence(IntList inputSequence) {
		addInputSequence(inputSequence, 0, inputSequence.size(), 1);
	}
	
	
	public void addInputSequence(IntList inputSequence, int fromIndex, int toIndex, int support) {
		int transactionId = transactionSupports.size();
		transactionSupports.add(support);

		int length = toIndex - fromIndex;
		int[] inputTransaction = new int[length];
		for (int i=0; i < length; i++)
			inputTransaction[i] = inputSequence.getInt(i+fromIndex);
		inputTransactions.add(inputTransaction);

		for (int i = fromIndex; i < toIndex; ++i) {
			int itemFid = inputSequence.getInt(i);
			assert itemFid  <= endItem;
			if (itemFid < 0) {
				continue;
			}

            if(largestFrequentFid >= itemFid) {
                globalItems.addItem(itemFid, transactionId, support, i);
                itemsCounted++;
            }

            if (generalize) {
                ascendants.clear();
                ctx.dict.addAscendantFids(ctx.dict.getItemByFid(itemFid), ascendants);
                IntIterator itemFidIt = ascendants.iterator();
                while (itemFidIt.hasNext()) {
                    itemFid = itemFidIt.nextInt();
                    if(largestFrequentFid >= itemFid) {
                        globalItems.addItem(itemFid, transactionId, support, i);
                        itemsCounted++;
                    }
                }
			}
		}
	}

	public void mine() {
		//System.out.println("Root items counted =" + itemsCounted);
		noOutputPatterns = 0;

		int[] prefix = new int[1];

		for (Map.Entry<Integer, Item> entry : globalItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				prefix[0] = entry.getKey();
				if (ctx.patternWriter != null) {
					ctx.patternWriter.write(new IntArrayList(prefix), item.support);
				}
				dfs(prefix, item.transactionIds, prefix[0] >= beginItem);
			}
		}
		clear();
	}

	private void dfs(int[] prefix, ByteArrayList transactionIds, boolean hasPivot) {
		if (prefix.length == lambda)
			return;
		PostingList.Decompressor transactions = new PostingList.Decompressor(transactionIds);

		Items localItems = new Items();

		do {
			int transactionId = transactions.nextValue();
			transaction = inputTransactions.get(transactionId);
            int transactionSupport = transactionSupports.get(transactionId);
			// for all positions
			while (transactions.hasNextValue()) {
				int position = transactions.nextValue();

				/* Add items in the right gamma+1 neighborhood */
				int gap = 0;
				for (int j = 0; gap <= gamma && (position + j + 1 < transaction.length); ++j) {
                    int newPosition = position + j + 1;
                    int itemFid = transaction[newPosition];
					if (itemFid < 0) {
						gap -= itemFid;
						continue;
					}
					gap++;

                    if (largestFrequentFid >= itemFid) {
                        localItems.addItem(itemFid, transactionId, transactionSupport, newPosition);
                        itemsCounted++;
                    }

					// add parents
					if (generalize) {
                        ascendants.clear();
                        ctx.dict.addAscendantFids(ctx.dict.getItemByFid(itemFid), ascendants);
                        IntIterator itemFidIt = ascendants.iterator();
                        while (itemFidIt.hasNext()) {
                            itemFid = itemFidIt.nextInt();
                            if(largestFrequentFid >= itemFid) {
                                localItems.addItem(itemFid, transactionId, transactionSupport, newPosition);
                                itemsCounted++;
                            }
                        }
					}

				}
			}

		} while (transactions.nextPosting());

		int[] newPrefix = new int[prefix.length + 1];

		for (Map.Entry<Integer, Item> entry : localItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				System.arraycopy(prefix, 0, newPrefix, 0, prefix.length);
				newPrefix[prefix.length] = entry.getKey();

				boolean containsPivot = hasPivot || (newPrefix[prefix.length] >= beginItem);

				if (containsPivot) {
					noOutputPatterns++;
					if (ctx.patternWriter != null) 
						ctx.patternWriter.write(new IntArrayList(newPrefix), item.support);
				}
				dfs(newPrefix, item.transactionIds, containsPivot);
			}
		}
		localItems.clear();
	}

	public int noOutputPatterns() {
		return noOutputPatterns;
	}

	
	// -- HELPER CLASSES --

	private static final class Item {

		int support;

		int lastTransactionId;

		int lastPosition;

		ByteArrayList transactionIds;

		Item() {
			support = 0;
			lastTransactionId = -1;
			lastPosition = -1;
			transactionIds = new ByteArrayList();
		}
	}

	private static final class Items {

		Int2ObjectOpenHashMap<Item> itemIndex = new Int2ObjectOpenHashMap<>();

		public void addItem(int itemFid, int transactionId, int support, int position) {
			
			Item item = itemIndex.get(itemFid);
			if (item == null) {
				item = new Item();
				itemIndex.put(itemFid, item);
				// baseItems.add(itemFid);
			}

			if (item.lastTransactionId != transactionId) {

				/** Add transaction separator */
				if (item.transactionIds.size() > 0) {
					PostingList.addCompressed(0, item.transactionIds);
				}
				item.lastPosition = position;
				item.lastTransactionId = transactionId;
				item.support += support;
				PostingList.addCompressed(transactionId + 1, item.transactionIds);
				PostingList.addCompressed(position + 1, item.transactionIds);

			} else if (item.lastPosition != position) {
				PostingList.addCompressed(position + 1, item.transactionIds);
				item.lastPosition = position;
			}
		}

		public void clear() {
			itemIndex.clear();
		}
	}
}
