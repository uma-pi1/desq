package de.uni_mannheim.desq.mining;

import java.util.ArrayList;
import java.util.Map;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import mining.PostingList;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 *
 * Gives wrong results (see DfsMiningExample). Also needs to be cleaned up.
 */
public class DfsMiner extends DesqMiner {
	// parameters for mining
	protected int sigma;
	protected int gamma;
	protected int lambda;
	protected boolean generalize = false;

	// helper variables
	protected ArrayList<int[]> inputTransactions = new ArrayList<int[]>();
	protected IntArrayList transactionSupports = new IntArrayList();
	private int noOutputPatterns = 0;
	protected int beginItem = 0;
	protected int endItem = Integer.MAX_VALUE;
	protected Items globalItems = new Items();
	private int[] transaction = null;
	public int itemsCounted = 0;
	int[] flist;

	public DfsMiner(DesqMinerContext ctx, int gamma, int lambda, boolean generalize) {
		super(ctx);
		this.sigma = ctx.sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.generalize = generalize;
		this.flist = ctx.dict.getFlist().toIntArray();
	}

	public void clear() {
		inputTransactions.clear();
		transactionSupports.clear();
		globalItems.clear();
	}

	public void setParameters(int sigma, int gamma, int lambda, boolean generalize) {
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
			int itemId = inputSequence.getInt(i);
			assert itemId  <= endItem;
			if (itemId < 0) {
				continue;
			}

			if(flist[itemId] >= sigma) {
				globalItems.addItem(itemId, transactionId, support, i);
				itemsCounted++;
			}
			if (generalize) {
				de.uni_mannheim.desq.dictionary.Item item = ctx.dict.getItemByFid(itemId);
				while (!item.parents.isEmpty()) {
					assert item.parents.size()==1;
					item = item.parents.get(0);
					itemId = item.fid;
					if(flist[itemId] >= sigma) {
						globalItems.addItem(item.fid, transactionId, support, i);
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
				dfs(prefix, item.transactionIds, (prefix[0] >= beginItem));
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
			// for all positions
			while (transactions.hasNextValue()) {
				int position = transactions.nextValue();

				/** Add items in the right gamma+1 neighborhood */
				int gap = 0;
				for (int j = 0; gap <= gamma && (position + j + 1 < transaction.length); ++j) {
					int itemId = transaction[position + j + 1];
					if (itemId < 0) {
						gap -= itemId;
						continue;
					}
					gap++;
					//if (globalItems.itemIndex.get(itemId).support >= sigma) {
					if (flist[itemId] >= sigma) {
						localItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), (position + j + 1));
						itemsCounted++;
					}
					// add parents
					if (generalize) {
						de.uni_mannheim.desq.dictionary.Item item = ctx.dict.getItemByFid(itemId);
						while (!item.parents.isEmpty()) {
							assert item.parents.size()==1;
							item = item.parents.get(0);
							itemId = item.id;
							//if (globalItems.itemIndex.get(itemId).support >= sigma) {
							if (flist[itemId] >= sigma) {
								localItems.addItem(itemId, transactionId, transactionSupports.get(transactionId),
										(position + j + 1));
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

		Int2ObjectOpenHashMap<Item> itemIndex = new Int2ObjectOpenHashMap<Item>();

		public void addItem(int itemId, int transactionId, int support, int position) {
			
			Item item = itemIndex.get(itemId);
			if (item == null) {
				item = new Item();
				itemIndex.put(itemId, item);
				// baseItems.add(itemId);
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
