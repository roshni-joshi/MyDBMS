package a1.mydbms.service;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import a1.mydbms.model.Transaction;
import a1.mydbms.util.Constants;
import a1.mydbms.util.Util;

public class TransactionManager {

	/**
	 * Temporary buffer storing all the resources for a given transaction
	 */
	public static Map<String, List<String>> buffer = new HashMap<String, List<String>>();
	
	/**
	 * Two-phase locking using table level lock.
	 * Find all the resources(i.e. tables) required for a transaction.
	 * Acquire locks on all required tables. Add resources(i.e. table data) to local buffer. 
	 * Then, execute transaction queries
	 * @param transaction Transaction class object containing transaction data
	 * @param queryProcessor QueryProcessor class object
	 * @return status message of transaction execution
	 * @throws Exception
	 */
	public static String executeTransaction(Transaction transaction, QueryProcessor queryProcessor) throws Exception {
		List<String> resources = getResources(transaction.getQueries(), queryProcessor);
		LockManager lockManager = new LockManager();
		boolean status = lockManager.acquireAllLock(resources, transaction.getTransactionId());
		if(!status) {
			return "Cannot start transaction. Required tables are in use";
		}
		addResourcesToBuffer(resources);
		for(String query : transaction.getQueries()) {
			if(query.contains(Constants.ROLLBACK)) {
				executeRollback();
			} else if(query.contains(Constants.COMMIT)) {
				executeCommit();
			} else {
				queryProcessor.processQuery(query, false, true, transaction.getTransactionId());
			}
		}
		return "Transaction completed";
	}
	
	/**
	 * Get all the resources(tables) required for entire transaction to complete
	 * @param queries list of transaction queries
	 * @param queryProcessor QueryProcessor class object
	 * @return List of all the tables required by transaction queries
	 * @throws Exception
	 */
	private static List<String> getResources(List<String> queries, QueryProcessor queryProcessor) throws Exception {
		List<String> resources = new ArrayList<String>();
		for(String query : queries) {
			if(query.startsWith(Constants.INSERT) || query.startsWith(Constants.UPDATE) 
					|| query.startsWith(Constants.DELETE) || query.startsWith(Constants.SELECT)) {
				String tablename = queryProcessor.processQuery(query, true, false, Constants.EMPTY);
				if(!resources.contains(tablename)) {
					resources.add(tablename);
				}
			}
		}
		resources.add(Constants.LOGS_FILE);
		return resources;
	}
	
	/**
	 * Add all the required resources(tables) to temporary storage
	 * @param resources
	 * @throws Exception
	 */
	private static void addResourcesToBuffer(List<String> resources) throws Exception {
		for(String tablename : resources) {
			BufferedReader br = Util.getFileReader(tablename, false);
			List<String> data = br.lines().collect(Collectors.toList());
			buffer.put(tablename, data);
			br.close();
		}
	}
	
	/**
	 * execute rollback and empty buffer
	 */
	private static void executeRollback() {
		buffer.clear();
	}
	
	/**
	 * execute commit. Write buffer data to text file for permanent change.
	 * @throws Exception
	 */
	private static void executeCommit() throws Exception {
		for(String tablename : buffer.keySet()) {
			Util.updateTableData(tablename, String.join(Constants.NEW_LINE, buffer.get(tablename)), false, false);
		}
		buffer.clear();
	}
}
