package a1.mydbms.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import a1.mydbms.model.Create;
import a1.mydbms.model.Delete;
import a1.mydbms.model.Insert;
import a1.mydbms.model.Select;
import a1.mydbms.model.Update;
import a1.mydbms.util.Constants;
import a1.mydbms.util.Util;

public class QueryProcessor {

	private QueryExecutor queryExecutor;
	
	public QueryProcessor(QueryExecutor queryExecutor) {
		this.queryExecutor = queryExecutor;
	}
	/**
	 * Validate the query. Check query type and call respective query processing method.
	 * @param query user query
	 * @param getResource true to fetch just the tablename from query else false
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return success message or error message for the query execution
	 * @throws Exception
	 */
	public String processQuery(String query, boolean getResource, boolean isTransactionQuery, String userId) {
		String result = Constants.EMPTY;
		try {
			if(query.startsWith(Constants.CREATE)) {
				result = processCreateQuery(query.replaceFirst(Constants.CREATE, Constants.EMPTY).trim(), userId);
			} else if(query.startsWith(Constants.INSERT)) {
				result = processInsertQuery(query.replaceFirst(Constants.INSERT, Constants.EMPTY).trim(), getResource, isTransactionQuery, userId);
			} else if(query.startsWith(Constants.SELECT)) {
				result = processSelectQuery(query.replaceFirst(Constants.SELECT, Constants.EMPTY).trim(), getResource, isTransactionQuery, userId);
			} else if(query.startsWith(Constants.UPDATE)) {
				result = processUpdateQuery(query.replaceFirst(Constants.UPDATE, Constants.EMPTY).trim(), getResource, isTransactionQuery, userId);
			} else if(query.startsWith(Constants.DELETE)) {
				result = processDeleteQuery(query.replaceFirst(Constants.DELETE, Constants.EMPTY).trim(), getResource, isTransactionQuery, userId);
			} else if(query.startsWith(Constants.CREATE_DB)) {
				result = "New databse cannot be created. Please use A1db only.";
			} else {
				result = "Invalid query";
			}
		} catch (Exception e) {
			return e.getMessage();
		}
		return result;
	}
	/**
	 * process create table query and call execute method
	 * @param query user query 
	 * @param userId id of the user who is executing the query
	 * @return success message or error message for the query execution
	 * @throws Exception
	 */
	private String processCreateQuery(String query, String userId) throws Exception {
		String tablename = query.substring(0, query.indexOf("(")).trim();
		if(!Util.isAlphaNumeric(tablename)) {
			return "Invalid table name";
		}
		Create create = new Create();
		Map<String, String> cols = new HashMap<String, String>();
		List<String> columns = Arrays.asList(query.substring(query.indexOf("(")+1, query.lastIndexOf(")")).split(","));
		for(String col : columns) {
			String primaryKey = Constants.EMPTY;
			String[] column = col.trim().split(" ");
			if(column.length == 2) {
				String name = column[0];
				String datatype = column[1];
				if(!Util.isAlphaNumeric(name)) {
					return "Invalid column name: " + name;
				}
				String size = "";
				if(datatype.contains("(")) {
					size = datatype.substring(datatype.indexOf("(")+1, datatype.indexOf(")"));
					if(!Util.isNumeric(size)) {
						return "Invalid size for column: " + name;
					}
					datatype = datatype.substring(0, datatype.indexOf("("));
				}
				
				if(!Constants.DATATYPES.contains(datatype)) {
					return "Invalid datatype " + datatype + " for colummn " + name;
				}
				if(datatype.equalsIgnoreCase("varchar") && size.isEmpty()) {
					return "Please provide size for column: " + name;
				}
				datatype = datatype + " " + size;
				cols.put(name, datatype.trim());
			}
			else if((col.trim().startsWith("primary key") || col.trim().startsWith("PRIMARY KEY")) && col.contains("(") && col.contains(")")) {
				primaryKey = col.substring(col.indexOf("(")+1, col.indexOf(")"));
				if(!cols.keySet().contains(primaryKey)) {
					return "Invalid primary key column: " + primaryKey;
				}
				create.setPrimaryKey(primaryKey);
			} else {
				return "Invalid Query format";
			}
		}
		if(!cols.keySet().contains(create.getPrimaryKey())) {
			return "Invalid primary key column";
		}
		create.setColummns(cols);
		create.setTablename(tablename);
		return queryExecutor.executeCreate(create, userId);
	}
	
	/**
	 * process insert query and call execute method
	 * @param query user query
	 * @param getResource true to fetch just the tablename from query else false
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return success message or error message for the query execution
	 * @throws Exception
	 */
	private String processInsertQuery(String query, boolean getResource, boolean isTransactionQuery, String userId) throws Exception {
		String[] queryParts = query.split("\\(");
		String tablename = queryParts[0].trim();
		if(!Util.isAlphaNumeric(tablename)) {
			return "Invalid table name";
		}
		if(getResource) {
			return tablename;
		}
		String[] columns = query.substring(query.indexOf("(") + 1, query.indexOf(")")).split(",");
		String[] values = query.substring(query.lastIndexOf("(") + 1, query.lastIndexOf(")")).split(",");
		if(columns.length != values.length) {
			return "Invalid columns and values mapping";
		}
		Map<String, String> data = IntStream.range(0, columns.length).boxed().collect(Collectors.toMap(i -> columns[i].trim(), i -> values[i].trim()));
		Insert insert = new Insert();
		insert.setTablename(tablename);
		insert.setData(data);
		return queryExecutor.executeInsert(insert, isTransactionQuery, userId);
	}
	
	/**
	 * process select query and call execute method
	 * @param query user query
	 * @param getResource true to fetch just the tablename from query else false
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return success message or error message for the query execution
	 * @throws Exception
	 */
	private String processSelectQuery(String query, boolean getResource, boolean isTransactionQuery, String userId) throws Exception {
		String tablename = query.split("from")[1].trim().split(Constants.SPACE)[0];
		if(!Util.isAlphaNumeric(tablename)) {
			return "Invalid table name";
		}
		if(getResource) {
			return tablename;
		}
		List<String> columns = Arrays.asList(query.split("from")[0].replaceAll(Constants.SPACE, Constants.EMPTY).split(","));
		String condition = Constants.EMPTY;
		if(query.contains("=")) {
			condition = query.split("where")[1].replaceAll(Constants.SPACE, Constants.EMPTY);
		}
		
		Select select = new Select();
		select.setTablename(tablename);
		select.setCondition(condition);
		select.setColumns(columns);
		return String.join(Constants.NEW_LINE, queryExecutor.executeSelect(select, isTransactionQuery, userId));
	}
	
	/**
	 * process update query and call execute method
	 * @param query user query
	 * @param getResource true to fetch just the tablename from query else false
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return success message or error message for the query execution
	 * @throws Exception
	 */
	private String processUpdateQuery(String query, boolean getResource, boolean isTransactionQuery, String userId) throws Exception {
		String tablename = query.split("set")[0].trim();
		String data = query.split("set")[1].replaceAll(Constants.SPACE, Constants.EMPTY);
		if(!Util.isAlphaNumeric(tablename)) {
			return "Invalid table name";
		}
		if(getResource) {
			return tablename;
		}
		String condition = Constants.EMPTY;
		if(query.contains("=")) {
			condition = query.split("where")[1].replaceAll(Constants.SPACE, Constants.EMPTY);
			data = data.split("where")[0].replaceAll(Constants.SPACE, Constants.EMPTY);
		}
		
		Update update = new Update();
		update.setTablename(tablename);
		update.setData(data);
		update.setCondition(condition);
		return queryExecutor.executeUpdate(update, isTransactionQuery, userId);
	}
	
	/**
	 * process delete query and call execute method
	 * @param query user query
	 * @param getResource true to fetch just the tablename from query else false
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return success message or error message for the query execution
	 * @throws Exception
	 */
	private String processDeleteQuery(String query, boolean getResource, boolean isTransactionQuery, String userId) throws Exception {
		String tablename = query;
		String condition = Constants.EMPTY;
		if(query.contains("=")) {
			tablename = query.split("where")[0].trim();
			condition = query.split("where")[1].replaceAll(Constants.SPACE, Constants.EMPTY);
		}
		if(!Util.isAlphaNumeric(tablename)) {
			return "Invalid table name";
		}
		if(getResource) {
			return tablename;
		}
		Delete delete = new Delete();
		delete.setCondition(condition);
		delete.setTablename(tablename);
		return queryExecutor.executeDelete(delete, isTransactionQuery, userId);
	}
}
