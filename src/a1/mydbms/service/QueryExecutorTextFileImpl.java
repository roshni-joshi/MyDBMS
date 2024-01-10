package a1.mydbms.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import a1.mydbms.model.Create;
import a1.mydbms.model.Delete;
import a1.mydbms.model.Insert;
import a1.mydbms.model.Select;
import a1.mydbms.model.Update;
import a1.mydbms.util.Constants;
import a1.mydbms.util.Util;

public class QueryExecutorTextFileImpl implements QueryExecutor {

	@Override
	public String executeCreate(Create create, String userId) throws Exception {
		File file = new File(System.getProperty("user.dir") + Constants.BASE_URL + create.getTablename() + Constants.TXT);
		if(file.exists()) {
			return "Table already exists";
		}
		file.createNewFile();
		Map<String, String> columnsData = create.getColummns();
		StringBuffer inputData = new StringBuffer();
		StringBuffer metadata = new StringBuffer();
		inputData.append(create.getPrimaryKey());
		metadata.append(columnsData.get(create.getPrimaryKey()));
		columnsData.remove(create.getPrimaryKey());
		for(String column : columnsData.keySet()) {
			inputData.append(Constants.PIPE_SPACE + column);
			metadata.append(Constants.PIPE_SPACE + columnsData.get(column));
		}
		PrintWriter pw = new PrintWriter(new FileOutputStream(file));
		pw.println(metadata.toString());
		pw.println(inputData.toString());
		pw.close();
		Util.writeLogs(Constants.CREATE + Constants.COLON_SPACE + create.getTablename() + Constants.COLON_SPACE + userId, false);
		return create.getTablename() + ": Table Created";
	}

	@Override
	public String executeInsert(Insert insert, boolean isTransactionQuery, String userId) throws Exception {
		StringBuffer inputData = new StringBuffer();
		BufferedReader br = Util.getFileReader(insert.getTablename(), isTransactionQuery);
		String[] metadata = br.readLine().split(Constants.PIPE_ESCAPE);
		String[] columns = br.readLine().split(Constants.PIPE_ESCAPE);
		Map<String, String> data = insert.getData();
		if(br.lines().anyMatch(s -> s.contains(data.get(columns[0]) + Constants.PIPE_SPACE))) {
			br.close();
			return "Data already exists";
		}
		br.close();
		for(int i=0; i < columns.length; i++) {
			String value = data.get(columns[i]);
			String[] metadataInfo = metadata[i].split(Constants.SPACE);
			
			if(!Util.validateDataType(value, metadataInfo[0])) {
				return "Invalid data type for " + value;
			}
			if(metadataInfo.length == 2 && !(value.length() <= Integer.parseInt(metadataInfo[1]))) {
				return "Invalid length for " + value;
			}
			inputData.append(Constants.PIPE_SPACE + value);
		}
		Util.updateTableData(insert.getTablename(), inputData.toString(), isTransactionQuery, true);
		Util.writeLogs(Constants.INSERT + Constants.COLON_SPACE + insert.getTablename() + Constants.COLON_SPACE + userId, isTransactionQuery);
		return insert.getTablename() + ": Insert executed";
	}

	@Override
	public List<String> executeSelect(Select select, boolean isTransactionQuery, String userId) throws Exception {
		BufferedReader br = Util.getFileReader(select.getTablename(), isTransactionQuery);
		br.readLine();
		List<String> tableColumns = Arrays.asList(br.readLine().split(Constants.PIPE_ESCAPE));
		List<String> selectedRows;
		if(!select.getCondition().isEmpty() && select.getCondition().contains(Constants.EQUALS)) {
			int colIndex = tableColumns.indexOf(select.getCondition().split(Constants.EQUALS)[0]);
			selectedRows = br.lines()
					.filter(row -> row.split(Constants.PIPE_ESCAPE)[colIndex].equals(select.getCondition().split(Constants.EQUALS)[1]))
					.collect(Collectors.toList());
			
		} else {
			selectedRows = br.lines().collect(Collectors.toList());
		}
		br.close();
		List<String> result = new ArrayList<String>();
		List<String> selectColumns = select.getColumns().get(0).equals("*") ? tableColumns : select.getColumns();
		result.add(String.join(Constants.PIPE_SPACE, selectColumns));
		result.add(Constants.LINE_SEPARATOR);
		selectedRows.stream().forEach(row -> {
			String[] data = row.split(Constants.PIPE_ESCAPE);
			StringBuffer buffer = new StringBuffer();
			for(String col : selectColumns) {
				buffer.append(Constants.PIPE_SPACE + data[tableColumns.indexOf(col)]);
			}
			result.add(buffer.toString().replaceFirst(Constants.PIPE_ESCAPE, Constants.EMPTY));
		});
		return result;
	}

	@Override
	public String executeUpdate(Update update, boolean isTransactionQuery, String userId) throws Exception {
		BufferedReader br = Util.getFileReader(update.getTablename(), isTransactionQuery);
		StringBuffer result = new StringBuffer();
		result.append(br.readLine());
		String columns = br.readLine();
		result.append(Constants.NEW_LINE + columns);
		List<String> tableColumns = Arrays.asList(columns.split(Constants.PIPE_ESCAPE));
		boolean hasCondition = (update.getCondition().isEmpty()) ? false : true;
		String updateColumn = update.getData().split(Constants.EQUALS)[0];
		String updateData = update.getData().split(Constants.EQUALS)[1];
		br.lines().forEach(row -> {
			String[] rowData = row.split(Constants.PIPE_ESCAPE);
			if(hasCondition) {
				int colIndex = tableColumns.indexOf(update.getCondition().split(Constants.EQUALS)[0]);
				if(rowData[colIndex].equals(update.getCondition().split(Constants.EQUALS)[1])) {
					rowData[tableColumns.indexOf(updateColumn)] = updateData;
				}
			} else {
				rowData[tableColumns.indexOf(updateColumn)] = updateData;
			}
			result.append(Constants.NEW_LINE + String.join(Constants.PIPE_SPACE, rowData));
		});
		br.close();
		Util.updateTableData(update.getTablename(), result.toString(), isTransactionQuery, false);
		Util.writeLogs(Constants.UPDATE + Constants.COLON_SPACE + update.getTablename() + Constants.COLON_SPACE + userId, isTransactionQuery);
		return update.getTablename() + ": Update executed"; 
	}

	@Override
	public String executeDelete(Delete delete, boolean isTransactionQuery, String userId) throws Exception {
		BufferedReader br = Util.getFileReader(delete.getTablename(), isTransactionQuery);
		StringBuffer result = new StringBuffer();
		result.append(br.readLine());
		String columns = br.readLine();
		result.append(Constants.NEW_LINE + columns);
		List<String> tableColumns = Arrays.asList(columns.split(Constants.PIPE_ESCAPE));
		if(!delete.getCondition().isEmpty()) {
			br.lines().forEach(row -> {
				int colIndex = tableColumns.indexOf(delete.getCondition().split(Constants.EQUALS)[0]);
				if(!row.split(Constants.PIPE_ESCAPE)[colIndex].equals(delete.getCondition().split(Constants.EQUALS)[1])) {
					result.append(Constants.NEW_LINE + row);
				}
			});
		}
		br.close();
		Util.updateTableData(delete.getTablename(), result.toString(), isTransactionQuery, false);
		Util.writeLogs(Constants.DELETE + Constants.COLON_SPACE + delete.getTablename() + Constants.COLON_SPACE + userId, isTransactionQuery);
		return delete.getTablename() + ": Delete executed"; 
	}

}
