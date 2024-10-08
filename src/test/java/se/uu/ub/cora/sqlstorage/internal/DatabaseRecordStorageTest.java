/*
 * Copyright 2021, 2022, 2023 Uppsala University Library
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
package se.uu.ub.cora.sqlstorage.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.postgresql.util.PGobject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataProvider;
import se.uu.ub.cora.data.DataRecordGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.converter.DataToJsonConverterProvider;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.data.spies.DataFactorySpy;
import se.uu.ub.cora.data.spies.DataGroupSpy;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqlstorage.spy.json.DataToJsonConverterFactoryCreatorSpy;
import se.uu.ub.cora.sqlstorage.spy.json.DataToJsonConverterFactorySpy;
import se.uu.ub.cora.sqlstorage.spy.json.DataToJsonConverterSpy;
import se.uu.ub.cora.sqlstorage.spy.json.JsonParserSpy;
import se.uu.ub.cora.sqlstorage.spy.json.JsonToDataConverterFactorySpy;
import se.uu.ub.cora.sqlstorage.spy.json.JsonToDataConverterSpy;
import se.uu.ub.cora.sqlstorage.spy.sql.RowSpy;
import se.uu.ub.cora.sqlstorage.spy.sql.SqlDatabaseFactorySpy;
import se.uu.ub.cora.sqlstorage.spy.sql.TableFacadeSpy;
import se.uu.ub.cora.sqlstorage.spy.sql.TableQuerySpy;
import se.uu.ub.cora.storage.Condition;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.Part;
import se.uu.ub.cora.storage.RecordConflictException;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RelationalOperator;
import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StorageReadResult;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class DatabaseRecordStorageTest {

	private static final List<String> LIST_OF_TYPES = List.of("someType1", "someType2");
	private static final List<String> LIST_WITH_ONE_TYPE = List.of("someType");
	private DatabaseRecordStorage storage;
	private SqlDatabaseFactorySpy sqlDatabaseFactorySpy;
	private JsonParserSpy jsonParserSpy;
	private JsonToDataConverterFactorySpy jsonToDataConverterFactory;
	private Filter filter;
	private Set<StorageTerm> emptyStorageTerms;
	private Set<Link> emptyLinkSet;
	private DataToJsonConverterFactoryCreatorSpy dataToJsonConverterFactoryCreatorSpy;
	private DataGroup dataRecord;
	private String dataDivider;
	private String someType;
	private String someId;
	private DataFactorySpy dataFactorySpy;

	@BeforeMethod
	public void beforeMethod() {
		dataFactorySpy = new DataFactorySpy();
		DataProvider.onlyForTestSetDataFactory(dataFactorySpy);
		jsonToDataConverterFactory = new JsonToDataConverterFactorySpy();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(jsonToDataConverterFactory);
		dataToJsonConverterFactoryCreatorSpy = new DataToJsonConverterFactoryCreatorSpy();
		DataToJsonConverterProvider
				.setDataToJsonConverterFactoryCreator(dataToJsonConverterFactoryCreatorSpy);

		filter = new Filter();
		emptyStorageTerms = new LinkedHashSet<>();
		emptyLinkSet = new LinkedHashSet<>();
		sqlDatabaseFactorySpy = new SqlDatabaseFactorySpy();
		sqlDatabaseFactorySpy.numberOfAffectedRows = 1;
		jsonParserSpy = new JsonParserSpy();
		storage = new DatabaseRecordStorage(sqlDatabaseFactorySpy, jsonParserSpy);

		dataRecord = new DataGroupSpy();
		dataDivider = "someDataDivider";

		someType = "someType";
		someId = "someId";
	}

	@Test
	public void testReadTableFacadeFactoredAndCloseCalled() throws Exception {
		storage.read("someType", "someId");
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testReadParametersAddedToTableQueryAndPassedOn() throws Exception {
		storage.read("someType", "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", "someType");
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "id", "someId");

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readOneRowForQuery", 0, tableQuerySpy);
	}

	@Test
	public void testReadTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwNotFoundExceptionFromTableFacadeOnRead = true;
		try {
			storage.read("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"No record found for recordType(s): someType, with id: someId.");
			assertEquals(e.getCause().getMessage(),
					"Not found error from readOneRowForQuery in tablespy");
		}
	}

	@Test
	public void testReadTypeOtherError() throws Exception {
		sqlDatabaseFactorySpy.throwDataExceptionFromTableFacadeOnRead = true;
		try {
			storage.read("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(), "Read did not generate a single result for recordType(s): "
					+ "someType, with id: someId.");
			assertEquals(e.getCause().getMessage(),
					"Data error from readOneRowForQuery in tablespy");
		}
	}

	@Test
	public void testReadOkReadJsonConvertedToDataGroup() throws Exception {
		DataRecordGroup readValueFromStorage = storage.read("someType", "someId");

		TableFacadeSpy tableFacade = (TableFacadeSpy) sqlDatabaseFactorySpy.MCR
				.getReturnValue("factorTableFacade", 0);

		RowSpy rowSpy = (RowSpy) tableFacade.MCR.getReturnValue("readOneRowForQuery", 0);
		rowSpy.MCR.assertParameters("getValueByColumn", 0, "data");
		var jsonRecord = rowSpy.MCR.getReturnValue("getValueByColumn", 0);

		jsonParserSpy.MCR.assertParameters("parseString", 0, jsonRecord);
		var jsonValue = jsonParserSpy.MCR.getReturnValue("parseString", 0);

		jsonToDataConverterFactory.MCR.assertParameters("createForJsonObject", 0, jsonValue);
		JsonToDataConverterSpy jsonToDataConverter = (JsonToDataConverterSpy) jsonToDataConverterFactory.MCR
				.getReturnValue("createForJsonObject", 0);

		jsonToDataConverter.MCR.assertParameters("toInstance", 0);
		var dataGroup = jsonToDataConverter.MCR.getReturnValue("toInstance", 0);

		dataFactorySpy.MCR.assertParameters("factorRecordGroupFromDataGroup", 0, dataGroup);
		dataFactorySpy.MCR.assertReturn("factorRecordGroupFromDataGroup", 0, readValueFromStorage);
	}

	@Test
	public void testOldReadTableFacadeFactoredAndCloseCalled() throws Exception {
		storage.read(List.of("someType", "someOtherType"), "someId");
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testOldReadParametersAddedToTableQueryAndPassedOn() throws Exception {
		List<String> types = List.of("someType", "someOtherType");
		storage.read(types, "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", types);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "id", "someId");

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readOneRowForQuery", 0, tableQuerySpy);
	}

	private TableQuerySpy getFactoredTableQueryUsingCallNumber(int callNumber) {
		return (TableQuerySpy) sqlDatabaseFactorySpy.MCR.getReturnValue("factorTableQuery",
				callNumber);
	}

	private TableFacadeSpy getFirstFactoredTableFacadeSpy() {
		return (TableFacadeSpy) sqlDatabaseFactorySpy.MCR.getReturnValue("factorTableFacade", 0);
	}

	@Test
	public void testOldReadTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwNotFoundExceptionFromTableFacadeOnRead = true;
		try {
			storage.read(List.of("someType", "someOtherType"), "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"No record found for recordType(s): [someType, someOtherType], with id: someId.");
			assertEquals(e.getCause().getMessage(),
					"Not found error from readOneRowForQuery in tablespy");
		}
	}

	@Test
	public void testOldReadTypeOtherError() throws Exception {
		sqlDatabaseFactorySpy.throwDataExceptionFromTableFacadeOnRead = true;
		try {
			storage.read(List.of("someType", "someOtherType"), "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(), "Read did not generate a single result for recordType(s): "
					+ "[someType, someOtherType], with id: someId.");
			assertEquals(e.getCause().getMessage(),
					"Data error from readOneRowForQuery in tablespy");
		}
	}

	@Test
	public void testOldReadOkReadJsonConvertedToDataGroup() throws Exception {
		DataGroup readValueFromStorage = storage.read(List.of("someType", "someOtherType"),
				"someId");

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		RowSpy readRow = (RowSpy) tableFacadeSpy.MCR.getReturnValue("readOneRowForQuery", 0);
		assertRowToDataGroupConvertion(0, readRow, readValueFromStorage);
	}

	@Test
	public void testReadListOneTypeNoResult() throws Exception {
		OnlyForTestDatabaseRecordStorage sql = new OnlyForTestDatabaseRecordStorage(null, null);

		sql.readList(someType, filter);

		sql.MCR.assertParameterAsEqual("readList", 0, "types", List.of(someType));
		sql.MCR.assertParameter("readList", 0, "filter", filter);

		dataFactorySpy.MCR.assertMethodNotCalled("factorRecordGroupFromDataGroup");
	}

	@Test
	public void testReadListOneType() throws Exception {
		OnlyForTestDatabaseRecordStorage sql = new OnlyForTestDatabaseRecordStorage(null, null);
		StorageReadResult storageReadResult = createStorageReadResultWithToDataGroups();
		sql.MRV.setDefaultReturnValuesSupplier("readList", () -> storageReadResult);
		sql.readList(someType, filter);

		dataFactorySpy.MCR.assertNumberOfCallsToMethod("factorRecordGroupFromDataGroup", 2);
		for (DataGroup dataGroup : storageReadResult.listOfDataGroups) {
			DataRecordGroup dataRecordGroup = (DataRecordGroup) dataFactorySpy.MCR
					.assertCalledParametersReturn("factorRecordGroupFromDataGroup", dataGroup);
			assertTrue(storageReadResult.listOfDataRecordGroups.contains(dataRecordGroup));
		}

		assertTrue(storageReadResult.listOfDataGroups.isEmpty());

	}

	private StorageReadResult createStorageReadResultWithToDataGroups() {
		StorageReadResult storageReadResult = new StorageReadResult();
		DataGroupSpy firstDataGroup = new DataGroupSpy();
		storageReadResult.listOfDataGroups.add(firstDataGroup);
		DataGroupSpy secondDataGroup = new DataGroupSpy();
		storageReadResult.listOfDataGroups.add(secondDataGroup);
		return storageReadResult;
	}

	private class OnlyForTestDatabaseRecordStorage extends DatabaseRecordStorage {
		public MethodCallRecorder MCR = new MethodCallRecorder();
		public MethodReturnValues MRV = new MethodReturnValues();

		public OnlyForTestDatabaseRecordStorage(SqlDatabaseFactory sqlDatabaseFactory,
				JsonParser jsonParser) {
			super(sqlDatabaseFactory, jsonParser);
			MCR.useMRV(MRV);
			MRV.setDefaultReturnValuesSupplier("readList", StorageReadResult::new);
		}

		@Override
		public StorageReadResult readList(List<String> types, Filter filter) {
			return (StorageReadResult) MCR.addCallAndReturnFromMRV("types", types, "filter",
					filter);
		}
	}

	@Test
	public void testReadListTableFacadeFactoredAndCloseCalled() throws Exception {
		storage.readList(LIST_WITH_ONE_TYPE, filter);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testReadListTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			storage.readList(LIST_OF_TYPES, filter);
			makeSureErrorIsThrownFromAboveStatements();
		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"RecordType: [someType1, someType2] not found in storage.");
			assertEquals(e.getCause().getMessage(), "Error from readRowsForQuery in tablespy");
		}
	}

	private void makeSureErrorIsThrownFromAboveStatements() {
		assertTrue(false);
	}

	@Test
	public void testReadListTableQueryFactoredAndTableFacadeCalled() throws Exception {
		storage.readList(LIST_WITH_ONE_TYPE, filter);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "recordstorageterm");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readRowsForQuery", 0, tableQuerySpy);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", LIST_WITH_ONE_TYPE);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListTableQueryFactoredAndTableFacadeCalledTypes() throws Exception {
		storage.readList(LIST_OF_TYPES, filter);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", LIST_OF_TYPES);
	}

	@Test
	public void testReadListReturnsAStorageReadResult() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 3;

		StorageReadResult result = storage.readList(LIST_WITH_ONE_TYPE, filter);

		assertNotNull(result);
		assertEquals(result.start, 0);
		assertEquals(result.totalNumberOfMatches, 3);
		assertEquals(result.listOfDataGroups.size(), 3);
	}

	@Test
	public void testRealListRowToDataConvertion() throws Exception {
		StorageReadResult result = storage.readList(LIST_WITH_ONE_TYPE, filter);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		List<?> readRows = (List<?>) tableFacadeSpy.MCR.getReturnValue("readRowsForQuery", 0);

		for (int i = 0; i < readRows.size(); i++) {
			assertRowToDataGroupConvertionForOneRow(result, readRows, i);
		}

	}

	private void assertRowToDataGroupConvertionForOneRow(StorageReadResult result, List<?> readRows,
			int i) {
		RowSpy readRow = (RowSpy) readRows.get(i);
		DataGroup readValueFromStorage = result.listOfDataGroups.get(i);

		assertRowToDataGroupConvertion(i, readRow, readValueFromStorage);
	}

	private void assertRowToDataGroupConvertion(int callNumber, RowSpy readRow,
			DataGroup readValueFromStorage) {
		assertGetValueByColumnParameters(readRow);
		assertParseStringParameters(callNumber, readRow);
		assertcreateForJsonObjectParameters(callNumber);
		assertToInstanceParameters(callNumber, readValueFromStorage);
	}

	private void assertParseStringParameters(int callNumber, RowSpy readRow) {
		Object dataRecord = readRow.MCR.getReturnValue("getValueByColumn", 0);
		jsonParserSpy.MCR.assertParameters("parseString", callNumber, dataRecord);
	}

	private void assertGetValueByColumnParameters(RowSpy readRow) {
		readRow.MCR.assertMethodWasCalled("getValueByColumn");
		readRow.MCR.assertParameters("getValueByColumn", 0, "data");
	}

	private void assertcreateForJsonObjectParameters(int callNumber) {
		JsonValue jsonValue = (JsonValue) jsonParserSpy.MCR.getReturnValue("parseString",
				callNumber);
		jsonToDataConverterFactory.MCR.assertParameters("createForJsonObject", callNumber,
				jsonValue);
	}

	private void assertToInstanceParameters(int callNumber, DataGroup readValueFromStorage) {
		JsonToDataConverterSpy jsonToDataConverterSpy = (JsonToDataConverterSpy) jsonToDataConverterFactory.MCR
				.getReturnValue("createForJsonObject", callNumber);
		jsonToDataConverterSpy.MCR.assertReturn("toInstance", 0, readValueFromStorage);
	}

	@Test
	public void testReadListWithFromNoAndToNoInFilter() throws Exception {
		filter.fromNo = 1;
		filter.toNo = 10;
		storage.readList(LIST_WITH_ONE_TYPE, filter);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertMethodNotCalled("setFromNo");
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 10L);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListWithFromNoAndToNoInFilterHigher() throws Exception {
		filter.fromNo = 10;
		filter.toNo = 100;
		storage.readList(LIST_WITH_ONE_TYPE, filter);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 10L);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 100L);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListWithFromNoInFilter() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;
		filter.fromNo = 10;
		StorageReadResult result = storage.readList(LIST_WITH_ONE_TYPE, filter);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 10L);
		tableQuerySpy.MCR.assertMethodNotCalled("setToNo");
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");

		assertEquals(result.start, 0);
		assertEquals(result.totalNumberOfMatches, 747);
		assertEquals(result.listOfDataGroups.size(), 3);
	}

	@Test
	public void testReadListWithToNoInFilter() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;
		filter.toNo = 3;
		StorageReadResult result = storage.readList(LIST_WITH_ONE_TYPE, filter);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertMethodNotCalled("setFromNo");
		assertEquals(result.totalNumberOfMatches, 747);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 3L);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListWithFilterHasOneIncludePartAndOneCondition() throws Exception {
		Filter filterWithIncludePart = createFilterWithOneIncludePartAndOneCondition();

		StorageReadResult result = storage.readList(LIST_WITH_ONE_TYPE, filterWithIncludePart);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "recordstorageterm");

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readRowsForQuery", 0, tableQuerySpy);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", LIST_WITH_ONE_TYPE);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "storageKey", "someKey");
		tableQuerySpy.MCR.assertParameters("addCondition", 2, "value", "someValue");
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
		tableQuerySpy.MCR.assertNumberOfCallsToMethod("addCondition", 3);

		assertTotalNumberOfRowsWithFilter(result.totalNumberOfMatches, tableFacadeSpy, 1);
	}

	private Filter createFilterWithOneIncludePartAndOneCondition() {
		Condition condition = new Condition("someKey", RelationalOperator.EQUAL_TO, "someValue");

		Part part = new Part();
		part.conditions.add(condition);

		Filter filterWithIncludePart = new Filter();
		filterWithIncludePart.include.add(part);
		return filterWithIncludePart;
	}

	private void assertTotalNumberOfRowsWithFilter(long totalNumberOfMatches,
			TableFacadeSpy tableFacadeSpy, int factorTableQueryCallNum) {
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", factorTableQueryCallNum,
				"recordstorageterm");
		TableQuerySpy tableQueryNumberOfRows = (TableQuerySpy) sqlDatabaseFactorySpy.MCR
				.getReturnValue("factorTableQuery", factorTableQueryCallNum);
		tableQueryNumberOfRows.MCR.assertParameters("addCondition", 0, "type", LIST_WITH_ONE_TYPE);
		tableQueryNumberOfRows.MCR.assertParameters("addCondition", 1, "storageKey", "someKey");
		tableQueryNumberOfRows.MCR.assertParameters("addCondition", 2, "value", "someValue");
		tableFacadeSpy.MCR.assertParameters("readNumberOfRows", 0, tableQueryNumberOfRows);
		tableFacadeSpy.MCR.assertReturn("readNumberOfRows", 0, totalNumberOfMatches);
	}

	@Test
	public void testGetTotalNumberOfRecordsForTypeTableFacadeFactoredAndCloseCalled()
			throws Exception {
		storage.getTotalNumberOfRecordsForTypes(LIST_OF_TYPES, filter);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");

		TableQuerySpy tableQuerySpy0 = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy0.MCR.assertParameters("addCondition", 0, "type", LIST_OF_TYPES);
	}

	@Test
	public void testGetTotalNumberOfRecordsForTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			storage.getTotalNumberOfRecordsForTypes(LIST_OF_TYPES, filter);
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"RecordType: [someType1, someType2] not found in storage.");
			assertEquals(e.getCause().getMessage(), "Error from readNumberOfRows in tablespy");
		}
	}

	@Test
	public void testGetTotalNumberOfRecordsForType() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;

		long count = storage.getTotalNumberOfRecordsForTypes(LIST_WITH_ONE_TYPE, filter);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();

		sqlDatabaseFactorySpy.MCR.assertParameter("factorTableQuery", 0, "tableName",
				"recordstorageterm");

		tableFacadeSpy.MCR.assertParameters("readNumberOfRows", 0, tableQuerySpy);
		tableFacadeSpy.MCR.assertReturn("readNumberOfRows", 0, count);
		assertEquals(count, 747);
	}

	@Test
	public void testGetTotalNumberOfRowsForTypesWithFilter() throws Exception {
		Filter filterWithIncludePart = createFilterWithOneIncludePartAndOneCondition();

		long count = storage.getTotalNumberOfRecordsForTypes(LIST_WITH_ONE_TYPE,
				filterWithIncludePart);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();

		assertTotalNumberOfRowsWithFilter(count, tableFacadeSpy, 0);

	}

	@Test

	public void testCreateTableFacadeFactoredAndTransactionAndCloseCalled() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		DataGroup dataRecord = new DataGroupSpy();
		String someDataDivider = "someDataDivider";

		storage.create("someType", "someId", dataRecord, emptyStorageTerms, emptyLinkSet,
				someDataDivider);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("startTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("endTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testCreateParametersPassedOnForRecord() throws Exception {
		storage.create(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet, dataDivider);

		String dataRecordJson = getConvertedJson(dataRecord);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addParameter", 0, "type", someType);
		tableQuerySpy.MCR.assertParameters("addParameter", 1, "id", someId);
		tableQuerySpy.MCR.assertParameters("addParameter", 2, "datadivider", dataDivider);

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecordJson);

		PGobject jsonObject2 = (PGobject) tableQuerySpy.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("addParameter", 3, "value");
		assertEquals(jsonObject2.getType(), "json");
		assertEquals(jsonObject2.getValue(), dataRecordJson);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 0, tableQuerySpy);
		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 1);
	}

	private String getConvertedJson(DataGroup dataRecord) {
		DataToJsonConverterFactorySpy converterFactorySpy = (DataToJsonConverterFactorySpy) dataToJsonConverterFactoryCreatorSpy.MCR
				.getReturnValue("createFactory", 0);

		converterFactorySpy.MCR.assertParameters("factorUsingConvertible", 0, dataRecord);

		DataToJsonConverterSpy dataToJsonConeverterSpy = (DataToJsonConverterSpy) converterFactorySpy.MCR
				.getReturnValue("factorUsingConvertible", 0);

		String dataRecordJson = (String) dataToJsonConeverterSpy.MCR.getReturnValue("toJson", 0);
		return dataRecordJson;
	}

	@Test
	public void testCreateParametersPassedOnForStorageTerm() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		Set<StorageTerm> storageTerms = createStorageTerms();

		storage.create(someType, someId, dataRecord, storageTerms, emptyLinkSet, dataDivider);

		sqlDatabaseFactorySpy.MCR.assertNumberOfCallsToMethod("factorTableQuery", 3);
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 1, "storageterm");
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "storageterm");
		TableQuerySpy tableQuery1 = getFactoredTableQueryUsingCallNumber(1);
		Object[] storageTermsArray = storageTerms.toArray();
		assertStorageTermAsTableQuery((StorageTerm) storageTermsArray[0], tableQuery1);

		TableQuerySpy tableQuery2 = getFactoredTableQueryUsingCallNumber(2);
		assertStorageTermAsTableQuery((StorageTerm) storageTermsArray[1], tableQuery2);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 3);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 1, tableQuery1);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 2, tableQuery2);
	}

	private Set<StorageTerm> createStorageTerms() {
		Set<StorageTerm> storageTerms = new LinkedHashSet<>();
		storageTerms.add(new StorageTerm("someStorageTermId", "someStorageKey", "someValue"));
		storageTerms.add(new StorageTerm("someStorageTermId", "someStorageKey2", "someValue2"));
		return storageTerms;
	}

	private void assertStorageTermAsTableQuery(StorageTerm storageTerm1,
			TableQuerySpy tableQuery1) {
		tableQuery1.MCR.assertParameters("addParameter", 0, "recordtype", someType);
		tableQuery1.MCR.assertParameters("addParameter", 1, "recordid", someId);
		tableQuery1.MCR.assertParameters("addParameter", 2, "storagetermid",
				storageTerm1.storageTermId());
		tableQuery1.MCR.assertParameters("addParameter", 3, "value", storageTerm1.value());
		tableQuery1.MCR.assertParameters("addParameter", 4, "storagekey",
				storageTerm1.storageKey());
	}

	@Test
	public void testCreateParametersPassedOnForLink() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		Set<Link> links = createLinks();

		storage.create(someType, someId, dataRecord, emptyStorageTerms, links, dataDivider);

		sqlDatabaseFactorySpy.MCR.assertNumberOfCallsToMethod("factorTableQuery", 3);
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 1, "link");
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "link");
		TableQuerySpy tableQuery1 = getFactoredTableQueryUsingCallNumber(1);
		assertLinkAsTableQuery((Link) links.toArray()[0], tableQuery1);
		TableQuerySpy tableQuery2 = getFactoredTableQueryUsingCallNumber(2);
		assertLinkAsTableQuery((Link) links.toArray()[1], tableQuery2);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 3);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 1, tableQuery1);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 2, tableQuery2);

	}

	private Set<Link> createLinks() {
		Set<Link> links = new LinkedHashSet<>();
		links.add(new Link("toType1", "toId1"));
		links.add(new Link("toType2", "toId2"));
		return links;
	}

	private void assertLinkAsTableQuery(Link link, TableQuerySpy tableQuery) {
		tableQuery.MCR.assertParameters("addParameter", 0, "fromtype", someType);
		tableQuery.MCR.assertParameters("addParameter", 1, "fromid", someId);
		tableQuery.MCR.assertParameters("addParameter", 2, "totype", link.type());
		tableQuery.MCR.assertParameters("addParameter", 3, "toid", link.id());
	}

	@Test
	public void testCreateThrowsRecordConflictException() throws Exception {
		sqlDatabaseFactorySpy.throwDuplicateExceptionFromTableFacade = true;

		try {
			storage.create(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();
		} catch (Exception e) {
			assertTrue(e instanceof RecordConflictException);
			assertEquals(e.getMessage(),
					"Record with type: someType, and id: someId already exists in storage.");
			assertEquals(e.getCause().getMessage(), "Error from insertRowUsingQuery in tablespy");

		}
	}

	@Test
	public void testCreateThrowsSQlDatabaseException() throws Exception {
		sqlDatabaseFactorySpy.throwSqlExceptionFromTableFacade = true;

		try {
			storage.create(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"Storage exception when creating record with recordType: someType and id: someId.");
			assertEquals(e.getCause().getMessage(), "Error from spy");

		}
	}

	@Test
	public void testUpdateClosed() throws Exception {
		storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet, dataDivider);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");

	}

	@Test
	public void testUpdateParametersAssertRecord() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;

		storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet, dataDivider);

		String dataRecordJson = getConvertedJson(dataRecord);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(2);
		tableQuerySpy.MCR.assertParameters("addParameter", 0, "datadivider", dataDivider);
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecordJson);

		PGobject jsonObject2 = (PGobject) tableQuerySpy.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("addParameter", 1, "value");
		assertEquals(jsonObject2.getType(), "json");
		assertEquals(jsonObject2.getValue(), dataRecordJson);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", someType);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "id", someId);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("startTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("endTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("close");

		tableFacadeSpy.MCR.assertParameters("updateRowsUsingQuery", 0, tableQuerySpy);
		tableFacadeSpy.MCR.assertNumberOfCallsToMethod("deleteRowsForQuery", 2);
		tableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 0);
	}

	@Test
	public void testUpdateAssertStoragTerm() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		Set<StorageTerm> storageTerms = createStorageTerms();

		storage.update(someType, someId, dataRecord, storageTerms, emptyLinkSet, dataDivider);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "storageterm");
		TableQuerySpy deleteTableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		deleteTableQuerySpy.MCR.assertParameters("addCondition", 0, "recordtype", "someType");
		deleteTableQuerySpy.MCR.assertParameters("addCondition", 1, "recordid", "someId");

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("deleteRowsForQuery", 0,
				deleteTableQuerySpy);

		sqlDatabaseFactorySpy.MCR.assertNumberOfCallsToMethod("factorTableQuery", 5);
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "storageterm");
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 3, "storageterm");
		TableQuerySpy tableQuery1 = getFactoredTableQueryUsingCallNumber(2);
		Object[] storageTermsArray = storageTerms.toArray();
		assertStorageTermAsTableQuery((StorageTerm) storageTermsArray[0], tableQuery1);

		TableQuerySpy tableQuery2 = getFactoredTableQueryUsingCallNumber(3);
		assertStorageTermAsTableQuery((StorageTerm) storageTermsArray[1], tableQuery2);

		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 2);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 0, tableQuery1);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 1, tableQuery2);
	}

	@Test
	public void testUpdateAssertLink() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		Set<Link> links = createLinks();
		storage.update(someType, someId, dataRecord, emptyStorageTerms, links, dataDivider);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 1, "link");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(1);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "fromtype", "someType");
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "fromid", "someId");

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("deleteRowsForQuery", 1, tableQuerySpy);

		sqlDatabaseFactorySpy.MCR.assertNumberOfCallsToMethod("factorTableQuery", 5);
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "link");
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 3, "link");
		TableQuerySpy tableQuery1 = getFactoredTableQueryUsingCallNumber(2);
		assertLinkAsTableQuery((Link) links.toArray()[0], tableQuery1);
		TableQuerySpy tableQuery2 = getFactoredTableQueryUsingCallNumber(3);
		assertLinkAsTableQuery((Link) links.toArray()[1], tableQuery2);

		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 2);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 0, tableQuery1);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 1, tableQuery2);
	}

	@Test
	public void testUpdateParameterRecordNotValidJson() throws Exception {

		storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet, dataDivider);

		String dataRecordJson = getConvertedJson(dataRecord);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(2);
		tableQuerySpy.MCR.assertParameters("addParameter", 0, "datadivider", dataDivider);
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecordJson);

		PGobject jsonObject2 = (PGobject) tableQuerySpy.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("addParameter", 1, "value");
		assertEquals(jsonObject2.getType(), "json");
		assertEquals(jsonObject2.getValue(), dataRecordJson);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", someType);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "id", someId);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();

		firstFactoredTableFacadeSpy.MCR.assertParameters("updateRowsUsingQuery", 0, tableQuerySpy);
	}

	@Test
	public void testUpdateTypeOrIdNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnUpdate = true;
		try {
			storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"Storage exception when updating record with recordType: someType and id: someId.");
			assertEquals(e.getCause().getMessage(), "Error from updateRowsUsingQuery in tablespy");
		}
	}

	@Test
	public void testUpdateNoRecordUpdated() throws Exception {
		sqlDatabaseFactorySpy.numberOfAffectedRows = 0;

		try {
			storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkSet,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"Record not found when updating record with recordType: someType and id: someId.");
		}
	}

	@Test
	public void testDeleteAssertRecord() {
		sqlDatabaseFactorySpy.usingTransaction = true;

		storage.deleteByTypeAndId("someType", "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(2);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", someType);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "id", someId);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("startTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("endTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("close");

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("deleteRowsForQuery", 2, tableQuerySpy);
	}

	@Test
	public void testDeleteAssertStoragTerm() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;

		storage.deleteByTypeAndId("someType", "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "storageterm");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "recordtype", "someType");
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "recordid", "someId");

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("deleteRowsForQuery", 0, tableQuerySpy);
	}

	@Test
	public void testDeleteAssertLinks() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;

		storage.deleteByTypeAndId("someType", "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 1, "link");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(1);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "fromtype", "someType");
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "fromid", "someId");

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("deleteRowsForQuery", 1, tableQuerySpy);
	}

	@Test
	public void testDeleteThrowsSQlDatabaseException() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnDelete = true;

		try {
			storage.deleteByTypeAndId("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"Storage exception when deleting record with recordType: someType and id: someId.");
			assertEquals(e.getCause().getMessage(), "Error from deleteRowsUsingQuery in tablespy");

		}
	}

	@Test
	public void testDeletedClosed() throws Exception {
		storage.deleteByTypeAndId("someType", "someId");
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testDeleteNoRecordUpdated() throws Exception {
		sqlDatabaseFactorySpy.numberOfAffectedRows = 0;

		try {
			storage.deleteByTypeAndId("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"Record not found when deleting record with recordType: someType and id: someId.");
		}
	}

	@Test
	public void testLinksExistForRecordUsesDependencies() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 0;

		storage.linksExistForRecord(someType, someId);

		sqlDatabaseFactorySpy.MCR.assertMethodWasCalled("factorTableFacade");
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "link");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "totype", someType);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "toid", someId);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readNumberOfRows", 0, tableQuerySpy);
	}

	@Test
	public void testLinksExistForRecordDatabaseIsClosed() throws Exception {
		storage.linksExistForRecord("someType", "someId");
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testLinksExistForRecordError() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;

		try {
			storage.linksExistForRecord("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"Could not determine if links exist for type: someType and id: someId.");
			assertTrue(e.getCause() instanceof Exception);
		}
	}

	@Test
	public void testLinksExistForRecordNoLinksFound() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 0;

		boolean exists = storage.linksExistForRecord("someType", "someId");

		assertFalse(exists);
	}

	@Test
	public void testLinksExistForRecordLinksFound() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 3;

		boolean exists = storage.linksExistForRecord("someType", "someId");

		assertTrue(exists);
	}

	@Test
	public void testGetLinksToRecordNoLinksFound() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 0;

		Collection<Link> links = storage.getLinksToRecord("someType", "someId");

		assertEquals(links.size(), 0);
	}

	@Test
	public void testGetLinksToRecordWithLinks() {

		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 3;

		Set<Link> links = storage.getLinksToRecord(someType, someId);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "link");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addCondition", 0, "totype", someType);
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "toid", someId);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readRowsForQuery", 0, tableQuerySpy);

		List<RowSpy> rows = (List<RowSpy>) tableFacadeSpy.MCR.getReturnValue("readRowsForQuery", 0);
		Object[] linksArray = links.toArray();
		Link link1 = (Link) linksArray[0];
		Link link2 = (Link) linksArray[1];
		Link link3 = (Link) linksArray[2];
		assertRows(rows, 0, link1.type(), link1.id());
		assertRows(rows, 1, link2.type(), link2.id());
		assertRows(rows, 2, link3.type(), link3.id());

		assertEquals(links.size(), 3);
	}

	private void assertRows(List<RowSpy> rows, int rowNumber, String linkFromType,
			String linkFromId) {
		rows.get(rowNumber).MCR.assertParameters("getValueByColumn", 0, "fromtype");
		rows.get(rowNumber).MCR.assertParameters("getValueByColumn", 1, "fromid");

		rows.get(rowNumber).MCR.assertReturn("getValueByColumn", 0, linkFromType);
		rows.get(rowNumber).MCR.assertReturn("getValueByColumn", 1, linkFromId);
	}

	@Test
	public void testGetLinksToRecordDatabaseIsClosed() throws Exception {
		storage.getLinksToRecord("someType", "someId");
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testGetLinksToRecordError() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;

		try {
			storage.getLinksToRecord("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(), "Could not get links for type: someType and id: someId.");
			assertTrue(e.getCause() instanceof Exception);
		}
	}

	@Test
	public void testRecordExists_notFound0() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 0;
		assertFalse(storage.recordExists(LIST_WITH_ONE_TYPE, "someId"));
	}

	@Test
	public void testRecordExists_TableFacadeFactoredAndCloseCalled() throws Exception {
		assertFalse(storage.recordExists(LIST_WITH_ONE_TYPE, "someId"));

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testRecordExists_NotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			assertFalse(storage.recordExists(LIST_OF_TYPES, "someId"));
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"RecordType: [someType1, someType2] with id: someId, not found in storage.");
			assertEquals(e.getCause().getMessage(), "Error from readNumberOfRows in tablespy");
		}
	}

	@Test
	public void testRecordExists() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 1;

		boolean recordExists = storage.recordExists(LIST_WITH_ONE_TYPE, "someId");

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();

		sqlDatabaseFactorySpy.MCR.assertParameter("factorTableQuery", 0, "tableName", "record");

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "type", "someType");
		tableQuerySpy.MCR.assertParameters("addCondition", 1, "id", "someId");

		tableFacadeSpy.MCR.assertParameters("readNumberOfRows", 0, tableQuerySpy);
		assertTrue(recordExists);
	}

	@Test
	public void testRecordExists_Found747() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;
		assertTrue(storage.recordExists(LIST_OF_TYPES, "someId"));
	}
}
