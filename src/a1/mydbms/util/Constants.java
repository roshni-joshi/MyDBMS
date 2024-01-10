package a1.mydbms.util;

import java.util.Arrays;
import java.util.List;

public class Constants {

	public static String EMPTY = "";
	public static String SPACE = " ";
	public static String COLON_SPACE = " : ";
	public static String PIPE_SPACE = " | ";
	public static String PIPE_ESCAPE = " \\| ";
	public static String LINE_SEPARATOR = "------------------------------------------------";
	public static String EQUALS = "=";
	public static String NEW_LINE = "\n";
	public static String SEMI_COLON = ";";
	public static final String OPEN_BRACKET = "(";
	public static final String DATE_FORMAT = "dd/MM/yyyy";
	public static String BASE_URL = "\\Data\\A1db\\";
	
	public static String USER_CREDENTIALS_FILE = "usercredentials";
	public static String LOGS_FILE = "logs";
	public static String TXT = ".txt";
	public static String CREATE = "create table";
	public static String INSERT = "insert into";
	public static String SELECT = "select";
	public static String UPDATE = "update";
	public static String DELETE = "delete from";
	public static String CREATE_DB = "create database";
	public static String BEGIN_TRANSACTION = "begin transaction";
	public static String END_TRANSACTION = "end transaction";
	public static String ROLLBACK = "rollback";
	public static String COMMIT = "commit";
	public static String STOP = "stop";
	
	public static String ALPHANUMERIC = "[a-zA-Z0-9]+$";
	public static String NUMERIC = "[0-9]+";
	public static String VARCHAR = "varchar";
	public static String INT = "int";
	public static String DATE = "date";
	public static List<String> DATATYPES = Arrays.asList("varchar", "int", "date");
}
