package mining.statistics.data;

import fst.XFst;

public class DesqTransactionData {
	int[] transaction;
	int transactionId;
	XFst xFst;
	
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
	
	public XFst getxFst() {
		return xFst;
	}
	public void setxFst(XFst xFst) {
		this.xFst = xFst;
	}
	
}
