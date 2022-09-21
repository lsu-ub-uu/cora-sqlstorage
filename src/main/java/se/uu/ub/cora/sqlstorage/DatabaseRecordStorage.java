/*
 * Copyright 2021, 2022 Uppsala University Library
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
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
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

	private static final String TYPE_COLUMN = "type";
	static final String RECORD = "record";
	private static final String ID_COLUMN = "id";
	private static final String DATA_DIVIDER_COLUMN = "datadivider";
	private static final String RECORD_DATA_COLUMN = "data";
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
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		tableQuery.addCondition(TYPE_COLUMN, type);
		tableQuery.addCondition(ID_COLUMN, id);
		return tableQuery;
	}

	private DataGroup convertRowToDataGroup(Row readRow) {
		String jsonRecord = (String) readRow.getValueByColumn(RECORD_DATA_COLUMN);
		JsonValue jsonValue = jsonParser.parseString(jsonRecord);
		JsonToDataConverter jsonToDataConverter = JsonToDataConverterProvider
				.getConverterUsingJsonObject(jsonValue);
		return (DataGroup) jsonToDataConverter.toInstance();
	}

	@Override
	public void create(String type, String id, DataGroup dataRecord, List<StorageTerm> storageTerms,
			List<Link> links, String dataDivider) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			tryToCreate(type, id, dataRecord, storageTerms, links, dataDivider, tableFacade);
		} catch (SqlConflictException e) {
			throw RecordConflictException.withMessageAndException(
					"Record with type: " + type + ", and id: " + id + " already exists in storage.",
					e);
		} catch (Exception e) {
			throw createStorageExceptionUsingAction(type, id, "creating", e);
		}
	}

	private void tryToCreate(String type, String id, DataGroup dataRecord,
			List<StorageTerm> storageTerms, List<Link> links, String dataDivider,
			TableFacade tableFacade) throws SQLException {
		tableFacade.startTransaction();
		createCreateQueryForRecordAndAddItToTableFacade(type, id, dataRecord, dataDivider,
				tableFacade);
		createCreateQueriesForStorageTermsAndAddThemToTableFacade(type, id, storageTerms,
				tableFacade);
		createCreateQueriesForLinksAndAddThemToTableFacade(type, id, links, tableFacade);
		tableFacade.endTransaction();
	}

	private void createCreateQueryForRecordAndAddItToTableFacade(String type, String id,
			DataGroup dataRecord, String dataDivider, TableFacade tableFacade) throws SQLException {
		String dataRecordJson = convertDataGroupToJsonString(dataRecord);
		TableQuery tableQuery = assembleCreateQuery(type, id, dataDivider, dataRecordJson);
		tableFacade.insertRowUsingQuery(tableQuery);
	}

	private void createCreateQueriesForStorageTermsAndAddThemToTableFacade(String type, String id,
			List<StorageTerm> storageTerms, TableFacade tableFacade) {
		for (StorageTerm storageTerm : storageTerms) {
			insertRowForStorageTerm(type, id, tableFacade, storageTerm);
		}
	}

	private void insertRowForStorageTerm(String type, String id, TableFacade tableFacade,
			StorageTerm storageTerm) {
		TableQuery storageTermsQuery = sqlDatabaseFactory.factorTableQuery("storageterm");
		addParemetersForStorageTerm(type, id, storageTerm, storageTermsQuery);
		tableFacade.insertRowUsingQuery(storageTermsQuery);
	}

	private void addParemetersForStorageTerm(String type, String id, StorageTerm storageTerm,
			TableQuery storageTermsQuery) {
		storageTermsQuery.addParameter("recordtype", type);
		storageTermsQuery.addParameter("recordid", id);
		storageTermsQuery.addParameter("storagetermid", storageTerm.id());
		storageTermsQuery.addParameter("value", storageTerm.value());
		storageTermsQuery.addParameter("storagekey", storageTerm.storageKey());
	}

	private void createCreateQueriesForLinksAndAddThemToTableFacade(String type, String id,
			List<Link> links, TableFacade tableFacade) {
		for (Link link : links) {
			insertRowForLink(type, id, tableFacade, link);
		}
	}

	private void insertRowForLink(String type, String id, TableFacade tableFacade, Link link) {
		TableQuery linkQuery = sqlDatabaseFactory.factorTableQuery("link");
		addParemetersForLink(type, id, link, linkQuery);
		tableFacade.insertRowUsingQuery(linkQuery);
	}

	private void addParemetersForLink(String type, String id, Link link, TableQuery linkQuery) {
		linkQuery.addParameter("fromtype", type);
		linkQuery.addParameter("fromid", id);
		linkQuery.addParameter("totype", link.type());
		linkQuery.addParameter("toid", link.id());
	}

	private StorageException createStorageExceptionUsingAction(String type, String id,
			String action, Exception exception) {
		return StorageException.withMessageAndException("Storage exception when " + action
				+ " record with recordType: " + type + " with id: " + id, exception);
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
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		tableQuery.addParameter(TYPE_COLUMN, type);
		tableQuery.addParameter(ID_COLUMN, id);
		tableQuery.addParameter(DATA_DIVIDER_COLUMN, dataDivider);
		PGobject jsonObject = createJsonObject(dataRecord);
		tableQuery.addParameter(RECORD_DATA_COLUMN, jsonObject);
		return tableQuery;
	}

	@Override
	public void deleteByTypeAndId(String type, String id) {
		int deletedRows = 0;
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			tableFacade.startTransaction();
			createDeleteQueryForStorageTermAndAddItToTableFacade(type, id, tableFacade);
			createDeleteQueryForLinkAndAddItToTableFacade(type, id, tableFacade);
			deletedRows = createDeleteQueryForRecordAndAddItToTableFacade(type, id, tableFacade);
			tableFacade.endTransaction();
		} catch (Exception e) {
			throw createStorageExceptionUsingAction(type, id, "deleting", e);
		}
		throwRecordNotFoundExceptionIfAffectedRowsIsZero(type, id, deletedRows, "deleting");
	}

	private void createDeleteQueryForStorageTermAndAddItToTableFacade(String type, String id,
			TableFacade tableFacade) {
		TableQuery storageTermQuery = sqlDatabaseFactory.factorTableQuery("storageterm");
		storageTermQuery.addCondition("recordtype", type);
		storageTermQuery.addCondition("recordid", id);
		tableFacade.deleteRowsForQuery(storageTermQuery);
	}

	private void createDeleteQueryForLinkAndAddItToTableFacade(String type, String id,
			TableFacade tableFacade) {
		TableQuery linkQuery = sqlDatabaseFactory.factorTableQuery("link");
		linkQuery.addCondition("fromtype", type);
		linkQuery.addCondition("fromid", id);
		tableFacade.deleteRowsForQuery(linkQuery);
	}

	private int createDeleteQueryForRecordAndAddItToTableFacade(String type, String id,
			TableFacade tableFacade) {
		int deletedRows;
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		tableQuery.addCondition(TYPE_COLUMN, type);
		tableQuery.addCondition(ID_COLUMN, id);
		deletedRows = tableFacade.deleteRowsForQuery(tableQuery);
		return deletedRows;
	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		throw NotImplementedException.withMessage("linksExistForRecord is not implemented");
	}

	@Override
	public void update(String type, String id, DataGroup dataRecord, List<StorageTerm> storageTerms,
			List<Link> links, String dataDivider) {
		int updatedRows = 0;
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			updatedRows = tryToUpdate(type, id, dataRecord, storageTerms, links, dataDivider,
					tableFacade);
		} catch (Exception e) {
			throw createStorageExceptionUsingAction(type, id, "updating", e);
		}
		throwRecordNotFoundExceptionIfAffectedRowsIsZero(type, id, updatedRows, "updating");
	}

	private int tryToUpdate(String type, String id, DataGroup dataRecord,
			List<StorageTerm> storageTerms, List<Link> links, String dataDivider,
			TableFacade tableFacade) throws SQLException {
		tableFacade.startTransaction();
		createDeleteQueryForStorageTermAndAddItToTableFacade(type, id, tableFacade);
		createDeleteQueryForLinkAndAddItToTableFacade(type, id, tableFacade);
		createCreateQueriesForStorageTermsAndAddThemToTableFacade(type, id, storageTerms,
				tableFacade);
		createCreateQueriesForLinksAndAddThemToTableFacade(type, id, links, tableFacade);
		int updatedRows = updateRecordData(type, id, dataRecord, dataDivider, tableFacade);
		tableFacade.endTransaction();
		return updatedRows;
	}

	private int updateRecordData(String type, String id, DataGroup dataRecord, String dataDivider,
			TableFacade tableFacade) throws SQLException {
		int updatedRows;
		String dataRecordJson = convertDataGroupToJsonString(dataRecord);
		TableQuery tableQuery = assembleUpdateQuery(type, id, dataDivider, dataRecordJson);
		updatedRows = tableFacade.updateRowsUsingQuery(tableQuery);
		return updatedRows;
	}

	private void throwRecordNotFoundExceptionIfAffectedRowsIsZero(String type, String id,
			int affectedRows, String storageAction) {
		if (affectedRows == 0) {
			throw new RecordNotFoundException("Record not found when " + storageAction
					+ " record with recordType: " + type + " and id: " + id);
		}
	}

	private TableQuery assembleUpdateQuery(String type, String id, String dataDivider,
			String dataRecord) throws SQLException {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		tableQuery.addParameter(DATA_DIVIDER_COLUMN, dataDivider);
		PGobject jsonObject = createJsonObject(dataRecord);

		tableQuery.addParameter(RECORD_DATA_COLUMN, jsonObject);
		tableQuery.addCondition(TYPE_COLUMN, type);
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
			throw createRecordNotFoundExceptionForType(type, e);
		}
	}

	private RecordNotFoundException createRecordNotFoundExceptionForType(String type,
			SqlDatabaseException e) {
		return new RecordNotFoundException("RecordType: " + type + ", not found in storage.", e);
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
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		possiblySetFromNoInQueryFromFilter(tableQuery, filter);
		possiblySetToNoInQueryFromFilter(tableQuery, filter);
		tableQuery.addOrderByDesc("id");
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
	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		throw NotImplementedException
				.withMessage("generateLinkCollectionPointingToRecord is not implemented");
	}

	@Override
	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String type,
			String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return tryToCheckIfRecordExistsForTypeAndId(type, id, tableFacade);
		} catch (SqlDatabaseException e) {
			throw createRecordNotFoundExceptionForExists(type, id, e);
		}
	}

	private boolean tryToCheckIfRecordExistsForTypeAndId(String type, String id,
			TableFacade tableFacade) {
		long numberOfRows = readNumberOfRowsFromDatabaseForTypeAndId(type, id, tableFacade);
		return numberOfRows > 0;
	}

	private long readNumberOfRowsFromDatabaseForTypeAndId(String type, String id,
			TableFacade tableFacade) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		tableQuery.addCondition(TYPE_COLUMN, type);
		tableQuery.addCondition(ID_COLUMN, id);
		return tableFacade.readNumberOfRows(tableQuery);
	}

	private RecordNotFoundException createRecordNotFoundExceptionForExists(String type, String id,
			SqlDatabaseException e) {
		String errorString = "RecordType: %s, with id: %s, not found in storage.";
		return new RecordNotFoundException(String.format(errorString, type, id), e);
	}

	@Override
	public long getTotalNumberOfRecordsForType(String type, DataGroup filter) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readNumberForType(type, tableFacade);
		} catch (SqlDatabaseException e) {
			throw createRecordNotFoundExceptionForType(type, e);
		}
	}

	private long readNumberForType(String type, TableFacade tableFacade) {
		return readFromDatabaseForTypeAndFilter(type, tableFacade);
	}

	private long readFromDatabaseForTypeAndFilter(String type, TableFacade tableFacade) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(RECORD);
		tableQuery.addCondition(TYPE_COLUMN, type);
		return tableFacade.readNumberOfRows(tableQuery);
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
