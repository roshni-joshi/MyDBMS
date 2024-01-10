package a1.mydbms.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LockManager {

	/**
	 * Store locked tables and respective transaction id
	 */
	Map<String, String> locks = new HashMap<String, String>();
	
	/**
	 * Lock all the required resources(tables) for a given transaction
	 * @param resources list of all tables used by given transaction
	 * @param transactionId unique id of a transaction acquiring locks 
	 * @return status of locking (true if all locks are acquired, else false)
	 */
	public boolean acquireAllLock(List<String> resources, String transactionId) {
		boolean status = true;
		for(String resource : resources) {
			if(locks.containsKey(resource)) {
				status = false;
				return status;
			}
		}
		resources.stream().forEach(resource -> locks.put(resource, transactionId));
		return status;
	}
	
	/**
	 * Release all the table locks acquired by a transaction
	 * @param transactionId transaction id for which locks to be released
	 */
	public void releaseAllLocks(String transactionId) {
		for(String resource : locks.keySet()) {
			if(locks.get(resource).equals(transactionId)) {
				locks.remove(resource);
			}
		}
		System.out.println("All locks released");
	}
}
