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
import java.text.MessageFormat;
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
import se.uu.ub.cora.sqldatabase.SqlDataException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.SqlNotFoundException;
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
	private static final String TABLE_RECORD = "record";
	private static final String TABLE_LINK = "link";

	private static final String FROMTYPE_COLUMN = "fromtype";
	private static final String FROMID_COLUMN = "fromid";
	private static final String TOTYPE_COLUMN = "totype";
	private static final String TOID_COLUMN = "toid";

	private static final String TYPE_COLUMN = "type";
	private static final String ID_COLUMN = "id";
	private static final String RECORD_DATA_COLUMN = "data";
	private static final String DATA_DIVIDER_COLUMN = "datadivider";
	private SqlDatabaseFactory sqlDatabaseFactory;
	private JsonParser jsonParser;

	public DatabaseRecordStorage(SqlDatabaseFactory sqlDatabaseFactory, JsonParser jsonParser) {
		this.sqlDatabaseFactory = sqlDatabaseFactory;
		this.jsonParser = jsonParser;
	}

	@Override
	public DataGroup read(List<String> types, String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readAndConvertData(types, id, tableFacade);
		} catch (SqlNotFoundException e) {
			throw new RecordNotFoundException(MessageFormat
					.format("No record found for recordType(s): {0}, with id: {1}.", types, id), e);
		} catch (SqlDataException e) {
			throw StorageException.withMessageAndException(MessageFormat.format(
					"Read did not generate a single result for recordType(s): {0}, with id: {1}.",
					types, id), e);
		}
	}

	private DataGroup readAndConvertData(List<String> types, String id, TableFacade tableFacade) {
		Row readRow = readFromDatabase(types, id, tableFacade);
		return convertRowToDataGroup(readRow);
	}

	private Row readFromDatabase(List<String> types, String id, TableFacade tableFacade) {
		TableQuery tableQuery = assembleReadOneQuery(types, id);
		return tableFacade.readOneRowForQuery(tableQuery);
	}

	private TableQuery assembleReadOneQuery(List<String> types, String id) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
		tableQuery.addCondition(TYPE_COLUMN, types);
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
			throw RecordConflictException.withMessageAndException(MessageFormat.format(
					"Record with type: {0}, and id: {1} already exists in storage.", type, id), e);
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
		addParametersForStorageTerm(type, id, storageTerm, storageTermsQuery);
		tableFacade.insertRowUsingQuery(storageTermsQuery);
	}

	private void addParametersForStorageTerm(String type, String id, StorageTerm storageTerm,
			TableQuery storageTermsQuery) {
		storageTermsQuery.addParameter("recordtype", type);
		storageTermsQuery.addParameter("recordid", id);
		storageTermsQuery.addParameter("storagetermid", storageTerm.storageTermId());
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
		TableQuery linkQuery = sqlDatabaseFactory.factorTableQuery(TABLE_LINK);
		addParemetersForLink(type, id, link, linkQuery);
		tableFacade.insertRowUsingQuery(linkQuery);
	}

	private void addParemetersForLink(String type, String id, Link link, TableQuery linkQuery) {
		linkQuery.addParameter(FROMTYPE_COLUMN, type);
		linkQuery.addParameter(FROMID_COLUMN, id);
		linkQuery.addParameter(TOTYPE_COLUMN, link.type());
		linkQuery.addParameter(TOID_COLUMN, link.id());
	}

	private StorageException createStorageExceptionUsingAction(String type, String id,
			String action, Exception exception) {
		return StorageException.withMessageAndException(MessageFormat.format(
				"Storage exception when {0} record with recordType: {1} and id: {2}.", action, type,
				id), exception);
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
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
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
		TableQuery linkQuery = sqlDatabaseFactory.factorTableQuery(TABLE_LINK);
		linkQuery.addCondition(FROMTYPE_COLUMN, type);
		linkQuery.addCondition(FROMID_COLUMN, id);
		tableFacade.deleteRowsForQuery(linkQuery);
	}

	private int createDeleteQueryForRecordAndAddItToTableFacade(String type, String id,
			TableFacade tableFacade) {
		int deletedRows;
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
		tableQuery.addCondition(TYPE_COLUMN, type);
		tableQuery.addCondition(ID_COLUMN, id);
		deletedRows = tableFacade.deleteRowsForQuery(tableQuery);
		return deletedRows;
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
			throw new RecordNotFoundException(MessageFormat.format(
					"Record not found when {0} record with recordType: {1} and id: {2}.",
					storageAction, type, id));
		}
	}

	private TableQuery assembleUpdateQuery(String type, String id, String dataDivider,
			String dataRecord) throws SQLException {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
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
	public StorageReadResult readList(List<String> types, DataGroup filter) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readAndConvertDataList(types, tableFacade, filter);
		} catch (SqlDatabaseException e) {
			throw createRecordNotFoundExceptionForType(types, e);
		}
	}

	private RecordNotFoundException createRecordNotFoundExceptionForType(List<String> types,
			SqlDatabaseException e) {
		String errMsg = MessageFormat.format("RecordType: {0} not found in storage.", types);
		return new RecordNotFoundException(errMsg, e);
	}

	private StorageReadResult readAndConvertDataList(List<String> types, TableFacade tableFacade,
			DataGroup filter) {
		List<Row> readRows = readRowsFromDatabase(types, tableFacade, filter);
		long totalNumberOfMatches = readNumberOfRows(types, tableFacade);
		StorageReadResult readResult = convertRowsToListOfDataGroups(readRows);
		readResult.totalNumberOfMatches = totalNumberOfMatches;
		return readResult;
	}

	private List<Row> readRowsFromDatabase(List<String> types, TableFacade tableFacade,
			DataGroup filter) {
		TableQuery tableQuery = assembleReadRowsQuery(types, filter);
		return tableFacade.readRowsForQuery(tableQuery);
	}

	private TableQuery assembleReadRowsQuery(List<String> types, DataGroup filter) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
		tableQuery.addCondition(TYPE_COLUMN, types);
		possiblySetFromNoInQueryFromFilter(tableQuery, filter);
		possiblySetToNoInQueryFromFilter(tableQuery, filter);
		tableQuery.addOrderByDesc("id");
		return tableQuery;
	}

	private void possiblySetFromNoInQueryFromFilter(TableQuery tableQuery, DataGroup filter) {
		if (filter.containsChildWithNameInData("fromNo")) {
			String fromNo = filter.getFirstAtomicValueWithNameInData("fromNo");
			tableQuery.setFromNo(Long.valueOf(fromNo));
		}
	}

	private void possiblySetToNoInQueryFromFilter(TableQuery tableQuery, DataGroup filter) {
		if (filter.containsChildWithNameInData("toNo")) {
			String toNo = filter.getFirstAtomicValueWithNameInData("toNo");
			tableQuery.setToNo(Long.valueOf(toNo));
		}
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
	public boolean linksExistForRecord(String type, String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return tryToDetermineIfLinksExistForRecord(type, id, tableFacade);
		} catch (Exception e) {
			throw StorageException.withMessageAndException(MessageFormat.format(
					"Could not determine if links exist for type: {0} and id: {1}.", type, id), e);
		}
	}

	private boolean tryToDetermineIfLinksExistForRecord(String type, String id,
			TableFacade tableFacade) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_LINK);
		tableQuery.addCondition(TOTYPE_COLUMN, type);
		tableQuery.addCondition(TOID_COLUMN, id);
		return tableFacade.readNumberOfRows(tableQuery) > 1;
	}

	@Override
	public Collection<Link> getLinksToRecord(String type, String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return tryToGetLinksToRecord(type, id, tableFacade);
		} catch (Exception e) {
			throw StorageException.withMessageAndException(MessageFormat
					.format("Could not get links for type: {0} and id: {1}.", type, id), e);
		}
	}

	private Collection<Link> tryToGetLinksToRecord(String type, String id,
			TableFacade tableFacade) {
		List<Row> readRowsForQuery = findLinksInStorage(tableFacade, type, id);
		return transformRowsToLinks(readRowsForQuery);
	}

	private List<Row> findLinksInStorage(TableFacade tableFacade, String type, String id) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_LINK);
		tableQuery.addCondition(TOTYPE_COLUMN, type);
		tableQuery.addCondition(TOID_COLUMN, id);
		return tableFacade.readRowsForQuery(tableQuery);
	}

	private Collection<Link> transformRowsToLinks(List<Row> readRowsForQuery) {
		List<Link> result = new ArrayList<>();
		for (Row row : readRowsForQuery) {
			String linkType = (String) row.getValueByColumn(FROMTYPE_COLUMN);
			String linkId = (String) row.getValueByColumn(FROMID_COLUMN);
			Link link = new Link(linkType, linkId);
			result.add(link);
		}
		return result;
	}

	@Override
	public boolean recordExists(List<String> types,
			String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return tryToCheckIfRecordExistsForTypeAndId(types, id, tableFacade);
		} catch (SqlDatabaseException e) {
			throw createRecordNotFoundExceptionForExists(types, id, e);
		}
	}

	private boolean tryToCheckIfRecordExistsForTypeAndId(List<String> types, String id,
			TableFacade tableFacade) {
		for (String type : types) {
			long numberOfRows = readNumberOfRowsFromDatabaseForTypeAndId(type, id, tableFacade);
			if (numberOfRows > 0) {
				return true;
			}
		}
		return false;
	}

	private long readNumberOfRowsFromDatabaseForTypeAndId(String type, String id,
			TableFacade tableFacade) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
		tableQuery.addCondition(TYPE_COLUMN, type);
		tableQuery.addCondition(ID_COLUMN, id);
		return tableFacade.readNumberOfRows(tableQuery);
	}

	private RecordNotFoundException createRecordNotFoundExceptionForExists(List<String> type,
			String id, SqlDatabaseException e) {
		return new RecordNotFoundException(MessageFormat
				.format("RecordType: {0} with id: {1}, not found in storage.", type, id), e);
	}

	@Override
	public long getTotalNumberOfRecordsForTypes(List<String> types, DataGroup filter) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			return readNumberOfRows(types, tableFacade);
		} catch (SqlDatabaseException e) {
			throw createRecordNotFoundExceptionForType(types, e);
		}
	}

	private long readNumberOfRows(List<String> types, TableFacade tableFacade) {
		TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(TABLE_RECORD);
		tableQuery.addCondition(TYPE_COLUMN, types);
		return tableFacade.readNumberOfRows(tableQuery);
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
