package a1.mydbms.model;

import java.util.Map;

public class Create extends Query {

	private Map<String, String> colummns;
	private String primaryKey;
	public Map<String, String> getColummns() {
		return colummns;
	}
	public void setColummns(Map<String, String> colummns) {
		this.colummns = colummns;
	}
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
}
