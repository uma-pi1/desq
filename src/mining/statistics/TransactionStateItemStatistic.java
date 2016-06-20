package mining.statistics;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.HashSet;
import java.util.stream.Collectors;

public class TransactionStateItemStatistic implements SPMLocalStatisticCollector {
	
	int support;
	int previousTransactionId;
	int maxTransactionLength;
	int eventsCount;
	Int2IntOpenHashMap localItemFrequencies = new Int2IntOpenHashMap();
//	HashMap<Integer, Integer> localItemFrequencies = new HashMap<Integer, Integer>();
	HashSet<Integer> pFSTStates = new HashSet<Integer>();
	
	public TransactionStateItemStatistic() {
		support = 0;
		previousTransactionId = -1;
		Collectors test; 

	}
	
	@Override
	public void onObservedItem(int transactionId, int[] transaction, int position, int pFSTState) {
		// update the item support and the item statistics
		if(transactionId != previousTransactionId) {
			support++;
			
			// update item statistics
			int currentItem;
			int toIndex = transaction.length;
			int fromIndex = position;
				
			while (fromIndex < toIndex) {
				currentItem = transaction[fromIndex];
				localItemFrequencies.put(currentItem, localItemFrequencies.get(currentItem) + 1);
				fromIndex++;
				eventsCount++;
			}
		}
		
		previousTransactionId = transactionId;
		
		// collect all possible pFSTStates
		if(!pFSTStates.contains(pFSTState)) {
			pFSTStates.add(pFSTState);
		}
		
		// determine the maximum transaction length  
		if(maxTransactionLength < (transaction.length - position)) {
			maxTransactionLength = transaction.length - position;
		}
	}
	
	public int getFrequency() {
		return support;
	}
	
	public int getMaxTransactionLength() {
		return maxTransactionLength;
	}

	public HashSet<Integer> getpFSTStates() {
		return pFSTStates;
	}
	
	public Int2IntOpenHashMap getLocalItemFrequencies() {
		return localItemFrequencies;
	}
	
	public int getEventsCount() {
		return eventsCount;
	}
}
