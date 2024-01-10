package a1.mydbms.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;

import a1.mydbms.util.Constants;

public class AuthenticationManager {

	/**
	 * Authenticate the user with userId and password
	 * @param userId a unique userId for each user
	 * @param password password of a user
	 * @return user authentication status (true - success, false - failure)
	 * @throws Exception
	 */
	public static boolean authenticate(String userId, String password) throws Exception {
		boolean isValid = false;
		File file = new File(System.getProperty("user.dir") + Constants.BASE_URL + Constants.USER_CREDENTIALS_FILE + Constants.TXT);
		BufferedReader br = new BufferedReader(new FileReader(file));
		if(br.lines().anyMatch(row -> row.split(Constants.SPACE)[0].equals(userId) && row.split(Constants.SPACE)[1].equals(encryptPassword(password)))) {
			isValid = true;
		}
		br.close();
		return isValid;
	}
	
	/**
	 * Check if user already exists or not and then register new user
	 * @param userId unique id of a user
	 * @param password user password
	 * @return registration message or error message if user already exist
	 * @throws Exception
	 */
	public static String register(String userId, String password) throws Exception {
		File file = new File(System.getProperty("user.dir") + Constants.BASE_URL + Constants.USER_CREDENTIALS_FILE + Constants.TXT);
		BufferedReader br = new BufferedReader(new FileReader(file));
		if(br.lines().anyMatch(row -> row.split(Constants.SPACE)[0].equals(userId))) {
			br.close();
			return "User already exists";
		}
		br.close();
		PrintWriter pw = new PrintWriter(new FileOutputStream(file, true));
		pw.println(userId + Constants.SPACE + encryptPassword(password));
		pw.close();
		return "Registered";
	}
	
	/**
	 * Encrypt user password using MD5 
	 * @param password user password
	 * @return encrypted password
	 */
	public static String encryptPassword(String password) {
		String hash = Constants.EMPTY;
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.update(password.getBytes());
			byte[] mddigest = messageDigest.digest();
			BigInteger bigInteger = new BigInteger(1, mddigest);
			hash = bigInteger.toString(16);
			while(hash.length() < 32) {
				hash = "0" + hash;
			}
		} catch (Exception e) {
			System.out.println("Exception occured while encrypting password");
			return Constants.EMPTY;
		}
		return hash;
	}
}
