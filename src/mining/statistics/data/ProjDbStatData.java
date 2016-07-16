package mining.statistics.data;

import java.util.HashMap;

import fst.XFst;
import mining.statistics.collectors.DesqGlobalDataCollector;

public class ProjDbStatData {
	
	int[] transaction;
	int transactionId;
	int stateFST;
	int position;
	HashMap<String,? extends DesqGlobalDataCollector<?,?>> globalDataCollectors;
	XFst xFst;
	boolean inFinalState;
	
	public int[] getTransaction() {
		return transaction;
	}
	public void setTransaction(int[] transaction) {
		this.transaction = transaction;
	}
	public int getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(int transactionId) {
		this.transactionId = transactionId;
	}
	public int getStateFST() {
		return stateFST;
	}
	public void setStateFST(int stateFST) {
		this.stateFST = stateFST;
	}
	public int getPosition() {
		return position;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	public boolean isInFinalState() {
		return inFinalState;
	}
	public void setInFinalState(boolean inFinalState) {
		this.inFinalState = inFinalState;
	}
	public XFst getxFst() {
		return xFst;
	}
	public void setxFst(XFst xFst) {
		this.xFst = xFst;
	}
	public HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> getGlobalDataCollectors() {
		return globalDataCollectors;
	}
	public void setGlobalDataCollectors(HashMap<String, ? extends DesqGlobalDataCollector<?, ?>> globalDataCollectors) {
		this.globalDataCollectors = globalDataCollectors;
	}
}
