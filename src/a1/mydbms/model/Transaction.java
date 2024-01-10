package a1.mydbms.model;

import java.util.List;

public class Transaction {

	private String transactionId;
	private List<String> queries;
	public String getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	public List<String> getQueries() {
		return queries;
	}
	public void setQueries(List<String> queries) {
		this.queries = queries;
	}
}
