package mining.statistics.old;

import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Iterator;

public class LocalItemFrequencyStatistic implements SPMStatisticsAggregator<Int2IntOpenHashMap,Int2IntOpenHashMap> {		
//		@Override
//		public Int2IntOpenHashMap initialize(int transactionId, int[] transaction, int position, int pFSTState) {
//			Int2IntOpenHashMap localItemFrequencies = new Int2IntOpenHashMap();
//			for (int i = 0; i < transaction.length; i++) {
//				localItemFrequencies.addTo(transaction[i], 1);
//			}
//			return localItemFrequencies;
//		}
//
//		@Override
//		public Int2IntOpenHashMap merge(Int2IntOpenHashMap p1, Int2IntOpenHashMap p2) {
//			for (Iterator<Entry> iterator = p1.int2IntEntrySet().fastIterator(); iterator.hasNext();) {
//				Entry entry = iterator.next();
//				p2.addTo(entry.getIntKey(), entry.getIntValue());
//			}
//			
//			return p2;
//		}
//
//		@Override
//		public Int2IntOpenHashMap terminate(Int2IntOpenHashMap p) {
//			return p;
//		}
}
