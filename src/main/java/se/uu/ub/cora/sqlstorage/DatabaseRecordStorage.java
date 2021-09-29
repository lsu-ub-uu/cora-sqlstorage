/*
 * Copyright 2021 Uppsala University Library
 *
 * This file is part of Cora.
 *
 *     Cora is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Cora is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.uu.ub.cora.sqlstorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.converter.JsonToDataConverter;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.sqldatabase.SqlDatabaseException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.table.TableFacade;
import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageReadResult;

/**
 * DatabaseRecordStorage provides an implementation of {@link RecordStorage} using a standardized
 * database schema to store records and metadata in the standard Cora JSON format. The database has
 * one table for each recordType. The tables consist of two columns id and dataRecord with the
 * record stored in JSON format.
 */
public class DatabaseRecordStorage implements RecordStorage {

	private static final String ID_COLUMN = "id";
	private static final String DATA_RECORD_COLUMN = "dataRecord";
	private SqlDatabaseFactory sqlDatabaseFactory;
	private JsonParser jsonParser;

	public DatabaseRecordStorage(SqlDatabaseFactory sqlDatabaseFactory, JsonParser jsonParser) {
		this.sqlDatabaseFactory = sqlDatabaseFactory;
		this.jsonParser = jsonParser;
	}

	@Override
	public DataGroup read(String type, String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readAndConvertData(type, id, tableFacade);
		} catch (SqlDatabaseException e) {
			throw new RecordNotFoundException(
					"No record found for recordType: " + type + " with id: " + id, e);
		}
	}

	private DataGroup readAndConvertData(String type, String id, TableFacade tableFacade) {
		Row readRow = readFromDatabase(type, id, tableFacade);
		return convertRowToDataGroup(readRow);
	}

	private Row readFromDatabase(String type, String id, TableFacade tableFacade) {
		TableQuery tableQuery = assembleReadOneQuery(type, id);
		return tableFacade.readOneRowForQuery(tableQuery);
	}

	private TableQuery assembleReadOneQuery(String type, String id) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(type);
		tableQuery.addCondition(ID_COLUMN, id);
		return tableQuery;
	}

	private DataGroup convertRowToDataGroup(Row readRow) {
		String jsonRecord = (String) readRow.getValueByColumn(DATA_RECORD_COLUMN);
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverter jsonToDataConverter = JsonToDataConverterProvider
				.getConverterUsingJsonObject(jsonValue);
		return (DataGroup) jsonToDataConverter.toInstance();
	}

	@Override
	public void create(String type, String id, DataGroup record, DataGroup collectedTerms,
			DataGroup linkList, String dataDivider) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteByTypeAndId(String type, String id) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void update(String type, String id, DataGroup record, DataGroup collectedTerms,
			DataGroup linkList, String dataDivider) {
		// TODO Auto-generated method stub

	}

	@Override
	public StorageReadResult readList(String type, DataGroup filter) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readAndConvertDataList(type, tableFacade, filter);
		} catch (SqlDatabaseException e) {
			throw new RecordNotFoundException("RecordType: " + type + ", not found in storage.", e);
		}
	}

	private StorageReadResult readAndConvertDataList(String type, TableFacade tableFacade,
			DataGroup filter) {
		List<Row> readRows = readRowsFromDatabase(type, tableFacade, filter);
		long totalNumberOfMatches = readNumberForTypeAndFilter(type, filter, tableFacade);
		StorageReadResult readResult = convertRowsToListOfDataGroups(readRows);
		readResult.totalNumberOfMatches = totalNumberOfMatches;
		return readResult;
	}

	private List<Row> readRowsFromDatabase(String type, TableFacade tableFacade, DataGroup filter) {
		TableQuery tableQuery = assembleReadRowsQuery(type, filter);
		return tableFacade.readRowsForQuery(tableQuery);
	}

	private void possiblySetToNoInQueryFromFilter(TableQuery tableQuery, DataGroup filter) {
		if (filter.containsChildWithNameInData("toNo")) {
			String toNo = filter.getFirstAtomicValueWithNameInData("toNo");
			tableQuery.setToNo(Long.parseLong(toNo));
		}
	}

	private void possiblySetFromNoInQueryFromFilter(TableQuery tableQuery, DataGroup filter) {
		if (filter.containsChildWithNameInData("fromNo")) {
			String fromNo = filter.getFirstAtomicValueWithNameInData("fromNo");
			tableQuery.setFromNo(Long.parseLong(fromNo));
		}
	}

	private TableQuery assembleReadRowsQuery(String type, DataGroup filter) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(type);
		possiblySetFromNoInQueryFromFilter(tableQuery, filter);
		possiblySetToNoInQueryFromFilter(tableQuery, filter);
		return tableQuery;
	}

	private StorageReadResult convertRowsToListOfDataGroups(List<Row> readRows) {
		StorageReadResult storageReadResult = createStorageReadResultForRows(readRows);
		convertAndAddRowToResult(readRows, storageReadResult);
		return storageReadResult;
	}

	private void convertAndAddRowToResult(List<Row> readRows, StorageReadResult storageReadResult) {
		List<DataGroup> dataGroups = new ArrayList<>();
		for (Row row : readRows) {
			dataGroups.add(convertRowToDataGroup(row));
		}
		storageReadResult.listOfDataGroups = dataGroups;
	}

	private StorageReadResult createStorageReadResultForRows(List<Row> readRows) {
		StorageReadResult storageReadResult = new StorageReadResult();
		return storageReadResult;
	}

	@Override
	public StorageReadResult readAbstractList(String type, DataGroup filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataGroup readLinkList(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String type,
			String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getTotalNumberOfRecordsForType(String type, DataGroup filter) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readNumberForTypeAndFilter(type, filter, tableFacade);
		} catch (SqlDatabaseException e) {
			throw new RecordNotFoundException("RecordType: " + type + ", not found in storage.", e);
		}
	}

	private long readNumberForTypeAndFilter(String type, DataGroup filter,
			TableFacade tableFacade) {
		return readFromDatabaseForTypeAndFilter(type, filter, tableFacade);
	}

	private long readFromDatabaseForTypeAndFilter(String type, DataGroup filter,
			TableFacade tableFacade) {
		TableQuery tableQuery = assembleCountQuery(type, filter);
		return tableFacade.readNumberOfRows(tableQuery);
	}

	private TableQuery assembleCountQuery(String type, DataGroup filter) {
		return sqlDatabaseFactory.factorTableQuery(type);
	}

	@Override
	public long getTotalNumberOfRecordsForAbstractType(String abstractType,
			List<String> implementingTypes, DataGroup filter) {
		// TODO Auto-generated method stub
		return 0;
	}

}
