package mining.statistics.data;

public class ProjDbStatData {
	int[] transaction;
	int transactionId;
	int stateFST;
	int position;
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
}
