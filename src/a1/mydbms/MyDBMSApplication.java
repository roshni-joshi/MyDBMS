package a1.mydbms;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import a1.mydbms.model.Transaction;
import a1.mydbms.service.AuthenticationManager;
import a1.mydbms.service.LockManager;
import a1.mydbms.service.QueryExecutorTextFileImpl;
import a1.mydbms.service.QueryProcessor;
import a1.mydbms.service.TransactionManager;
import a1.mydbms.util.Constants;
import a1.mydbms.util.Util;

public class MyDBMSApplication {

	/**
	 * Main method to run the application
	 * @param args command line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int option = 0;
		do {
			System.out.println(Constants.NEW_LINE + "Please select from below option :");
			System.out.println("1. Login \n2. Register \n3. Logout");
			option = Integer.parseInt(br.readLine());
			switch (option) {
			case 1: {
				System.out.println(Constants.NEW_LINE + "Enter userId: ");
				String userId = br.readLine();
				System.out.println("Enter password: ");
				String password = br.readLine();
				String captcha = Util.generateCaptcha();
				System.out.println("Enter captcha ("+captcha+"): ");
				if(br.readLine().equals(captcha)) {
					boolean status = AuthenticationManager.authenticate(userId, password);
					if(status) {
						QueryProcessor queryProcessor = new QueryProcessor(new QueryExecutorTextFileImpl());
						System.out.println(Constants.NEW_LINE + "Press '"+Constants.STOP+"' to exit");
						System.out.println("Enter Query:"); 
						String query = br.readLine().replaceAll(Constants.SEMI_COLON, Constants.EMPTY);
						while(!Constants.STOP.equalsIgnoreCase(query)) {
							if(Constants.BEGIN_TRANSACTION.equalsIgnoreCase(query.trim())) {
								List<String> queries = new ArrayList<String>();
								query = br.readLine().replaceAll(Constants.SEMI_COLON, Constants.EMPTY);
								while(!Constants.END_TRANSACTION.equalsIgnoreCase(query.trim())) {
									queries.add(query);
									query = br.readLine().replaceAll(Constants.SEMI_COLON, Constants.EMPTY);
								}
								Transaction transaction = new Transaction();
								transaction.setQueries(queries);
								transaction.setTransactionId(userId);
								System.out.println(TransactionManager.executeTransaction(transaction, queryProcessor));
								//Release all locks acquired by transaction
								LockManager lockManager = new LockManager();
								lockManager.releaseAllLocks(userId);
							} else {
								System.out.println(queryProcessor.processQuery(query, false, false, userId));
							}
							System.out.println(Constants.NEW_LINE + "Enter Query:"); 
							query = br.readLine().replaceAll(Constants.SEMI_COLON, Constants.EMPTY);
						}
					} else {
						System.out.println("Invalid credentials");
					}
				} else {
					System.out.println("Invalid captcha");
				}
				break;
			} case 2: {
				System.out.println(Constants.NEW_LINE + "Enter new userId: ");
				String userId = br.readLine();
				System.out.println("Enter new password: ");
				String password = br.readLine();
				String captcha = Util.generateCaptcha();
				System.out.println("Enter captcha ("+captcha+"): ");
				if(br.readLine().equals(captcha)) {
					String status = AuthenticationManager.register(userId, password);
					System.out.println(status);
				} else {
					System.out.println("Invalid captcha");
				}
				break;
			} case 3: {
				System.out.println(Constants.NEW_LINE + "Successfully logged out");		
				break;
			}
			default:
				System.out.println(Constants.NEW_LINE + "Invalid option");
			}
		} while(option != 3);
		br.close();
	}

}
