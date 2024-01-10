package a1.mydbms.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.regex.Pattern;

import a1.mydbms.service.TransactionManager;

public class Util {

	/**
	 * check whether given string is alphanumeric or not
	 * @param str input string
	 * @return true if given string is alphanumeric else false
	 */
	public static boolean isAlphaNumeric(String str) {
		return Pattern.matches(Constants.ALPHANUMERIC, str);
	}
	
	/**
	 * check whether given string is numeric or not
	 * @param str input string
	 * @return true if given string is numeric else false
	 */
	public static boolean isNumeric(String str) {
		return Pattern.matches(Constants.NUMERIC, str);
	}
	
	/**
	 * validate the datatype of given input data
	 * @param value input data
	 * @param datatype input value datatype 
	 * @return true if datatype is validated else false
	 */
	public static boolean validateDataType(String value, String datatype) {
		boolean result = true;
		if(datatype.equalsIgnoreCase(Constants.VARCHAR) && !isAlphaNumeric(value)) {
			result = false;
		} else if(datatype.equalsIgnoreCase(Constants.INT) && !isNumeric(value)) {
			result = false;
		} else if(datatype.equalsIgnoreCase(Constants.DATE)) {
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DATE_FORMAT);
				dateFormat.parse(value);
			} catch (ParseException e) {
				result = false;
				System.out.println("invalid date format");
			}
		}
		return result;
	}
	
	/**
	 * Generate captcha for authentication
	 * @return captcha string
	 */
	public static String generateCaptcha() {
		String alphaNumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		int size = 7;
		StringBuffer captcha = new StringBuffer();
		while(size-- > 0) {
			int index = (int)(Math.random()*62);
			captcha.append(alphaNumeric.charAt(index));
		}
		return captcha.toString();
	}
	
	/**
	 * validate tablename and fetch table data (If transaction query - then fetch data from buffer)
	 * @param tablename table name
	 * @param isTransactionQuery true if query is transaction query else false
	 * @return BufferedReader object containing table data
	 * @throws Exception
	 */
	public static BufferedReader getFileReader(String tablename, boolean isTransactionQuery) throws Exception {
		BufferedReader br;
		if(isTransactionQuery) {
			br = new BufferedReader(new StringReader(String.join(Constants.NEW_LINE, TransactionManager.buffer.get(tablename))));
		} else {
			File file = new File(System.getProperty("user.dir") + Constants.BASE_URL + tablename + Constants.TXT);
			if(!file.exists()) {
				throw new Exception("Table is not present");
			}
			br = new BufferedReader(new FileReader(file));
		}
		return br;
	}
	
	/**
	 * update query data in table (update buffer if transaction query, else update text file)
	 * @param tablename table name
	 * @param inputData input data to be updated in table
	 * @param isTransactionQuery true if query is transaction query else false
	 * @param isInsert true if query is insert query else false
	 * @throws Exception
	 */
	public static void updateTableData(String tablename, String inputData, boolean isTransactionQuery, boolean isInsert) throws Exception {
		File file = new File(System.getProperty("user.dir") + Constants.BASE_URL + tablename + Constants.TXT);
		if(isTransactionQuery) {
			if(isInsert) {
				TransactionManager.buffer.get(tablename).add(inputData.replaceFirst(Constants.PIPE_ESCAPE, Constants.EMPTY));
			} else {
				TransactionManager.buffer.put(tablename, Arrays.asList(inputData.split(Constants.NEW_LINE)));
			}
		} else {
			if(isInsert) {
				PrintWriter pw = new PrintWriter(new FileOutputStream(file, true));
				pw.println(inputData.replaceFirst(Constants.PIPE_ESCAPE, Constants.EMPTY));
				pw.close();
			} else {
				PrintWriter pw = new PrintWriter(new FileOutputStream(file));
				pw.print(inputData);
				pw.close();
			}
		}
	}
	
	/**
	 * write logs to logs.txt file
	 * @param input logging data
	 * @param isTransactionQuery true if query is transaction query else false
	 * @throws Exception
	 */
	public static void writeLogs(String input, boolean isTransactionQuery) throws Exception {
		if(isTransactionQuery) {
			TransactionManager.buffer.get(Constants.LOGS_FILE).add(input);
		} else {
			File file = new File(System.getProperty("user.dir") + Constants.BASE_URL + Constants.LOGS_FILE + Constants.TXT);
			PrintWriter pw = new PrintWriter(new FileOutputStream(file, true));
			pw.println(input);
			pw.close();
		}
	}
}
