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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.postgresql.util.PGobject;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.converter.DataToJsonConverter;
import se.uu.ub.cora.data.converter.DataToJsonConverterFactory;
import se.uu.ub.cora.data.converter.DataToJsonConverterProvider;
import se.uu.ub.cora.data.converter.JsonToDataConverter;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.sqldatabase.SqlConflictException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.table.TableFacade;
import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.storage.RecordConflictException;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StorageReadResult;

/**
 * DatabaseRecordStorage provides an implementation of {@link RecordStorage} using a standardized
 * database schema to store records and metadata in the standard Cora JSON format. The database has
 * one table for each recordType. The tables are named with a prefix "record_" and then the name of
 * the record type, ie "record_nameoftherecordtype" and consist of three columns id, datadivider and
 * dataRecord with the record stored in JSON format.
 * <p>
 * This implementation of RecordStorage is threadsafe.
 */
public class DatabaseRecordStorage implements RecordStorage {

	private static final String ID_COLUMN = "id";
	private static final String DATA_DIVIDER_COLUMN = "datadivider";
	private static final String DATA_RECORD_COLUMN = "record";
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
		TableQuery tableQuery = factorTableQueryWithTablePrefix(type);
		tableQuery.addCondition(ID_COLUMN, id);
		return tableQuery;
	}

	private TableQuery factorTableQueryWithTablePrefix(String type) {
		return sqlDatabaseFactory.factorTableQuery("record_" + type);
	}

	private DataGroup convertRowToDataGroup(Row readRow) {
		String jsonRecord = (String) readRow.getValueByColumn(DATA_RECORD_COLUMN);
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverter jsonToDataConverter = JsonToDataConverterProvider
				.getConverterUsingJsonObject(jsonValue);
		return (DataGroup) jsonToDataConverter.toInstance();
	}

	@Override
	public void create(String type, String id, DataGroup dataRecord, DataGroup collectedTerms,
			DataGroup linkList, String dataDivider) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			String dataRecordJson = convertDataGroupToJsonString(dataRecord);
			TableQuery tableQuery = assembleCreateQuery(type, id, dataDivider, dataRecordJson);
			tableFacade.insertRowUsingQuery(tableQuery);
		} catch (SqlConflictException e) {
			throw RecordConflictException.withMessageAndException(
					"Record with type: " + type + ", and id: " + id + " already exists in storage.",
					e);
		} catch (Exception e) {
			throw StorageException.withMessageAndException(
					"Storage exception when updating record with recordType: " + type + " with id: "
							+ id,
					e);
		}
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataToJsonConverter dataToJsonConverter = createDataGroupToJsonConvert(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataToJsonConverter createDataGroupToJsonConvert(DataGroup dataGroup) {
		DataToJsonConverterFactory converterFactory = DataToJsonConverterProvider
				.createImplementingFactory();
		return converterFactory.factorUsingConvertible(dataGroup);
	}

	private TableQuery assembleCreateQuery(String type, String id, String dataDivider,
			String dataRecord) throws SQLException {
		TableQuery tableQuery = factorTableQueryWithTablePrefix(type);
		tableQuery.addParameter(ID_COLUMN, id);
		tableQuery.addParameter(DATA_DIVIDER_COLUMN, dataDivider);
		PGobject jsonObject = createJsonObject(dataRecord);
		tableQuery.addParameter(DATA_RECORD_COLUMN, jsonObject);
		return tableQuery;
	}

	@Override
	public void deleteByTypeAndId(String type, String id) {
		throw NotImplementedException.withMessage("deleteByTypeAndId is not implemented");
	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		throw NotImplementedException.withMessage("linksExistForRecord is not implemented");
	}

	@Override
	public void update(String type, String id, DataGroup dataRecord, DataGroup collectedTerms,
			DataGroup linkList, String dataDivider) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			String dataRecordJson = convertDataGroupToJsonString(dataRecord);
			TableQuery tableQuery = assembleUpdateQuery(type, id, dataDivider, dataRecordJson);
			tableFacade.updateRowsUsingQuery(tableQuery);
		} catch (Exception e) {
			throw StorageException.withMessageAndException(
					"Storage exception when updating record with recordType: " + type + " with id: "
							+ id,
					e);
		}
	}

	private TableQuery assembleUpdateQuery(String type, String id, String dataDivider,
			String dataRecord) throws SQLException {
		TableQuery tableQuery = factorTableQueryWithTablePrefix(type);
		tableQuery.addParameter(DATA_DIVIDER_COLUMN, dataDivider);
		PGobject jsonObject = createJsonObject(dataRecord);

		tableQuery.addParameter(DATA_RECORD_COLUMN, jsonObject);
		tableQuery.addCondition(ID_COLUMN, id);
		return tableQuery;
	}

	private PGobject createJsonObject(String dataRecord) throws SQLException {
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecord);
		return jsonObject;
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
		long totalNumberOfMatches = readNumberForType(type, tableFacade);
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
			tableQuery.setToNo(Long.valueOf(toNo));
		}
	}

	private void possiblySetFromNoInQueryFromFilter(TableQuery tableQuery, DataGroup filter) {
		if (filter.containsChildWithNameInData("fromNo")) {
			String fromNo = filter.getFirstAtomicValueWithNameInData("fromNo");
			tableQuery.setFromNo(Long.valueOf(fromNo));
		}
	}

	private TableQuery assembleReadRowsQuery(String type, DataGroup filter) {
		TableQuery tableQuery = factorTableQueryWithTablePrefix(type);
		possiblySetFromNoInQueryFromFilter(tableQuery, filter);
		possiblySetToNoInQueryFromFilter(tableQuery, filter);
		return tableQuery;
	}

	private StorageReadResult convertRowsToListOfDataGroups(List<Row> readRows) {
		StorageReadResult storageReadResult = new StorageReadResult();
		convertAndAddRowToResult(readRows, storageReadResult);
		return storageReadResult;
	}

	private void convertAndAddRowToResult(List<Row> readRows, StorageReadResult storageReadResult) {
		List<DataGroup> dataGroups = new ArrayList<>(readRows.size());
		for (Row row : readRows) {
			dataGroups.add(convertRowToDataGroup(row));
		}
		storageReadResult.listOfDataGroups = dataGroups;
	}

	@Override
	public StorageReadResult readAbstractList(String type, DataGroup filter) {
		throw NotImplementedException.withMessage("readAbstractList is not implemented");
	}

	@Override
	public DataGroup readLinkList(String type, String id) {
		throw NotImplementedException.withMessage("readLinkList is not implemented");
	}

	@Override
	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		throw NotImplementedException
				.withMessage("generateLinkCollectionPointingToRecord is not implemented");
	}

	@Override
	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String type,
			String id) {
		throw NotImplementedException.withMessage(
				"recordExistsForAbstractOrImplementingRecordTypeAndRecordId is not implemented");
	}

	@Override
	public long getTotalNumberOfRecordsForType(String type, DataGroup filter) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readNumberForType(type, tableFacade);
		} catch (SqlDatabaseException e) {
			throw new RecordNotFoundException("RecordType: " + type + ", not found in storage.", e);
		}
	}

	private long readNumberForType(String type, TableFacade tableFacade) {
		return readFromDatabaseForTypeAndFilter(type, tableFacade);
	}

	private long readFromDatabaseForTypeAndFilter(String type, TableFacade tableFacade) {
		TableQuery tableQuery = assembleCountQuery(type);
		return tableFacade.readNumberOfRows(tableQuery);
	}

	private TableQuery assembleCountQuery(String type) {
		return factorTableQueryWithTablePrefix(type);
	}

	@Override
	public long getTotalNumberOfRecordsForAbstractType(String abstractType,
			List<String> implementingTypes, DataGroup filter) {
		throw NotImplementedException
				.withMessage("getTotalNumberOfRecordsForAbstractType is not implemented");
	}

	public SqlDatabaseFactory onlyForTestGetSqlDatabaseFactory() {
		// Needed for test
		return sqlDatabaseFactory;
	}

	public JsonParser onlyForTestGetJsonParser() {
		// Needed for test
		return jsonParser;
	}

}
