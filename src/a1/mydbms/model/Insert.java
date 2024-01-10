package a1.mydbms.model;

import java.util.Map;

public class Insert extends Query {

	private Map<String, String> data;
	public Map<String, String> getData() {
		return data;
	}
	public void setData(Map<String, String> data) {
		this.data = data;
	}
}
