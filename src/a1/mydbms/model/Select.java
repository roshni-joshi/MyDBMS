package a1.mydbms.model;

import java.util.List;

public class Select extends Query {

	private List<String> columns;
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
}
