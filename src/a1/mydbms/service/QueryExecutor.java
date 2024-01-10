package a1.mydbms.service;

import java.util.List;

import a1.mydbms.model.Create;
import a1.mydbms.model.Delete;
import a1.mydbms.model.Insert;
import a1.mydbms.model.Select;
import a1.mydbms.model.Update;

public interface QueryExecutor {
	/**
	 * execute create query
	 * @param create Create class object
	 * @param userId id of the user who is executing the query
	 * @return query execution status message
	 * @throws Exception
	 */
	String executeCreate(Create create, String userId) throws Exception;
	
	/**
	 * execute insert query
	 * @param insert Insert class object
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return query execution status message
	 * @throws Exception
	 */
	String executeInsert(Insert insert, boolean isTransactionQuery, String userId) throws Exception;
	
	/**
	 * execute select query
	 * @param select Select class object
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return query execution status message
	 * @throws Exception
	 */
	List<String> executeSelect(Select select, boolean isTransactionQuery, String userId) throws Exception;
	
	/**
	 * execute update query
	 * @param update Update class object
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return query execution status message
	 * @throws Exception
	 */
	String executeUpdate(Update update, boolean isTransactionQuery, String userId) throws Exception;
	
	/**
	 * execute delete query
	 * @param delete Delete class object
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param userId id of the user who is executing the query
	 * @return query execution status message
	 * @throws Exception
	 */
	String executeDelete(Delete delete, boolean isTransactionQuery, String userId) throws Exception;
}
