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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.postgresql.util.PGobject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.converter.DataToJsonConverterProvider;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.sqlstorage.spy.data.DataGroupSpy;
import se.uu.ub.cora.sqlstorage.spy.data.FilterDataGroupSpy;
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
import se.uu.ub.cora.storage.RecordConflictException;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.StorageException;
import se.uu.ub.cora.storage.StorageReadResult;

public class DatabaseRecordStorageTest {

	private DatabaseRecordStorage storage;
	private SqlDatabaseFactorySpy sqlDatabaseFactorySpy;
	private JsonParserSpy jsonParserSpy;
	private JsonToDataConverterFactorySpy factoryCreatorSpy;
	private DataGroup emptyFilterSpy;
	private FilterDataGroupSpy filterSpy;
	private List<StorageTerm> emptyStorageTerms;
	private List<Link> emptyLinkList;
	private DataToJsonConverterFactoryCreatorSpy dataToJsonConverterFactoryCreatorSpy;
	private DataGroup dataRecord;
	private String dataDivider;
	private String someType;
	private String someId;

	@BeforeMethod
	public void beforeMethod() {
		filterSpy = new FilterDataGroupSpy();
		emptyFilterSpy = new DataGroupSpy();
		emptyStorageTerms = Collections.emptyList();
		emptyLinkList = new ArrayList<>();
		factoryCreatorSpy = new JsonToDataConverterFactorySpy();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(factoryCreatorSpy);
		dataToJsonConverterFactoryCreatorSpy = new DataToJsonConverterFactoryCreatorSpy();
		DataToJsonConverterProvider
				.setDataToJsonConverterFactoryCreator(dataToJsonConverterFactoryCreatorSpy);
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

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", "someId");

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
	public void testReadTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			storage.read("someType", "someId");
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"No record found for recordType: someType with id: someId");
			assertEquals(e.getCause().getMessage(), "Error from readOneRowForQuery in tablespy");
		}
	}

	@Test
	public void testReadOkReadJsonConvertedToDataGroup() throws Exception {
		String recordType = "someRecordType";
		String id = "someId";

		DataGroup readValueFromStorage = storage.read(recordType, id);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();

		RowSpy readRow = (RowSpy) tableFacadeSpy.MCR.getReturnValue("readOneRowForQuery", 0);

		assertRowToDataGroupConvertion(0, readRow, readValueFromStorage);
	}

	@Test
	public void testReadListTableFacadeFactoredAndCloseCalled() throws Exception {

		storage.readList("someType", emptyFilterSpy);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testReadListTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			storage.readList("someType", emptyFilterSpy);
			makeSureErrorIsThrownFromAboveStatements();
		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(), "RecordType: someType, not found in storage.");
			assertEquals(e.getCause().getMessage(), "Error from readRowsForQuery in tablespy");
		}
	}

	private void makeSureErrorIsThrownFromAboveStatements() {
		assertTrue(false);
	}

	@Test
	public void testReadListTableQueryFactoredAndTableFacadeCalled() throws Exception {

		storage.readList("someType", emptyFilterSpy);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readRowsForQuery", 0, tableQuerySpy);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListReturnsAStorageReadResult() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 3;

		StorageReadResult result = storage.readList("someType", emptyFilterSpy);

		assertNotNull(result);
		assertEquals(result.start, 0);
		assertEquals(result.totalNumberOfMatches, 3);
		assertEquals(result.listOfDataGroups.size(), 3);
	}

	@Test
	public void testRealListRowToDataConvertion() throws Exception {
		StorageReadResult result = storage.readList("someType", emptyFilterSpy);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		List<RowSpy> readRows = (List) tableFacadeSpy.MCR.getReturnValue("readRowsForQuery", 0);

		for (int i = 0; i < readRows.size(); i++) {
			assertRowToDataGroupConvertionForOneRow(result, readRows, i);
		}

	}

	private void assertRowToDataGroupConvertionForOneRow(StorageReadResult result,
			List<RowSpy> readRows, int i) {
		RowSpy readRow = readRows.get(i);
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
		readRow.MCR.assertParameters("getValueByColumn", 0, "record");
	}

	private void assertcreateForJsonObjectParameters(int callNumber) {
		JsonValue jsonValue = (JsonValue) jsonParserSpy.MCR.getReturnValue("parseString",
				callNumber);
		factoryCreatorSpy.MCR.assertParameters("createForJsonObject", callNumber, jsonValue);
	}

	private void assertToInstanceParameters(int callNumber, DataGroup readValueFromStorage) {
		JsonToDataConverterSpy jsonToDataConverterSpy = (JsonToDataConverterSpy) factoryCreatorSpy.MCR
				.getReturnValue("createForJsonObject", callNumber);
		jsonToDataConverterSpy.MCR.assertReturn("toInstance", 0, readValueFromStorage);
	}

	@Test
	public void testReadListWithFromNoAndToNoInFilter() throws Exception {
		filterSpy.fromNo = "1";
		filterSpy.toNo = "10";
		storage.readList("someType", filterSpy);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 1L);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 10L);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListWithFromNoAndToNoInFilterHigher() throws Exception {
		filterSpy.fromNo = "10";
		filterSpy.toNo = "100";
		storage.readList("someType", filterSpy);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 10L);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 100L);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testReadListWithFromNoInFilter() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;
		filterSpy.fromNo = "10";
		StorageReadResult result = storage.readList("someType", filterSpy);

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
		filterSpy.toNo = "3";
		StorageReadResult result = storage.readList("someType", filterSpy);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertMethodNotCalled("setFromNo");
		assertEquals(result.totalNumberOfMatches, 747);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 3L);
		tableQuerySpy.MCR.assertParameter("addOrderByDesc", 0, "column", "id");
	}

	@Test
	public void testGetTotalNumberOfRecordsForTypeTableFacadeFactoredAndCloseCalled()
			throws Exception {
		storage.getTotalNumberOfRecordsForType("somType", emptyFilterSpy);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testGetTotalNumberOfRecordsForTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			storage.getTotalNumberOfRecordsForType("someType", emptyFilterSpy);
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(), "RecordType: someType, not found in storage.");
			assertEquals(e.getCause().getMessage(), "Error from readNumberOfRows in tablespy");
		}
	}

	@Test
	public void testGetTotalNumberOfRecordsForType() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;

		long count = storage.getTotalNumberOfRecordsForType("someType", emptyFilterSpy);

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();

		sqlDatabaseFactorySpy.MCR.assertParameter("factorTableQuery", 0, "tableName", "record");

		tableFacadeSpy.MCR.assertParameters("readNumberOfRows", 0, tableQuerySpy);
		tableFacadeSpy.MCR.assertReturn("readNumberOfRows", 0, count);
		assertEquals(count, 747);
	}

	@Test

	public void testCreateTableFacadeFactoredAndTransactionAndCloseCalled() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		DataGroup dataRecord = new DataGroupSpy();
		String someDataDivider = "someDataDivider";

		storage.create("someType", "someId", dataRecord, emptyStorageTerms, emptyLinkList,
				someDataDivider);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("startTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("endTransaction");
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test

	public void testCreateParametersPassedOnForRecord() throws Exception {
		storage.create(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList, dataDivider);

		String dataRecordJson = getConvertedJson(dataRecord);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addParameter", 0, "id", someId);
		tableQuerySpy.MCR.assertParameters("addParameter", 1, "datadivider", dataDivider);

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecordJson);

		PGobject jsonObject2 = (PGobject) tableQuerySpy.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("addParameter", 2, "value");
		assertEquals(jsonObject2.getType(), "json");
		assertEquals(jsonObject2.getValue(), dataRecordJson);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 0, tableQuerySpy);
		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 1);
	}

	private String getConvertedJson(DataGroup dataRecord) {
		DataToJsonConverterFactorySpy dataToJsonConverterFactorySpy = (DataToJsonConverterFactorySpy) dataToJsonConverterFactoryCreatorSpy.MCR
				.getReturnValue("createFactory", 0);

		dataToJsonConverterFactorySpy.MCR.assertParameters("factorUsingConvertible", 0, dataRecord);

		DataToJsonConverterSpy dataToJsonConeverterSpy = (DataToJsonConverterSpy) dataToJsonConverterFactorySpy.MCR
				.getReturnValue("factorUsingConvertible", 0);

		String dataRecordJson = (String) dataToJsonConeverterSpy.MCR.getReturnValue("toJson", 0);
		return dataRecordJson;
	}

	@Test
	public void testCreateParametersPassedOnForLink() throws Exception {
		sqlDatabaseFactorySpy.usingTransaction = true;
		StorageTerm storageTerm1 = new StorageTerm("someStorageTermId", "someValue",
				"someStorageKey");
		StorageTerm storageTerm2 = new StorageTerm("someStorageTermId", "someValue2",
				"someStorageKey2");
		List<StorageTerm> storageTerms = List.of(storageTerm1, storageTerm2);

		storage.create(someType, someId, dataRecord, storageTerms, emptyLinkList, dataDivider);

		sqlDatabaseFactorySpy.MCR.assertNumberOfCallsToMethod("factorTableQuery", 3);
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 1, "storageterm");
		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 2, "storageterm");
		TableQuerySpy tableQuery1 = getFactoredTableQueryUsingCallNumber(1);
		assertStorageTermsAsTableQueries(storageTerm1, tableQuery1);

		TableQuerySpy tableQuery2 = getFactoredTableQueryUsingCallNumber(2);
		assertStorageTermsAsTableQueries(storageTerm2, tableQuery2);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();
		firstFactoredTableFacadeSpy.MCR.assertNumberOfCallsToMethod("insertRowUsingQuery", 3);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 1, tableQuery1);
		firstFactoredTableFacadeSpy.MCR.assertParameters("insertRowUsingQuery", 2, tableQuery2);
	}

	private void assertStorageTermsAsTableQueries(StorageTerm storageTerm1,
			TableQuerySpy tableQuery1) {
		tableQuery1.MCR.assertParameters("addParameter", 0, "recordtype", someType);
		tableQuery1.MCR.assertParameters("addParameter", 1, "recordid", someId);
		tableQuery1.MCR.assertParameters("addParameter", 2, "storagetermid", storageTerm1.id());
		tableQuery1.MCR.assertParameters("addParameter", 3, "value", storageTerm1.value());
		tableQuery1.MCR.assertParameters("addParameter", 4, "storagekey",
				storageTerm1.storageKey());
	}

	@Test
	public void testCreateThrowsRecordConflictException() throws Exception {
		sqlDatabaseFactorySpy.throwDuplicateExceptionFromTableFacade = true;

		try {
			storage.create(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList,
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
			storage.create(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();
		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"Storage exception when creating record with recordType: someType with id: someId");
			assertEquals(e.getCause().getMessage(), "Error from spy");

		}
	}

	@Test
	public void testUpdateClosed() throws Exception {
		storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList, dataDivider);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");

	}

	@Test
	public void testUpdateParametersPassedOn() throws Exception {

		storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList, dataDivider);

		String dataRecordJson = getConvertedJson(dataRecord);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addParameter", 0, "datadivider", dataDivider);
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecordJson);

		PGobject jsonObject2 = (PGobject) tableQuerySpy.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("addParameter", 1, "value");
		assertEquals(jsonObject2.getType(), "json");
		assertEquals(jsonObject2.getValue(), dataRecordJson);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", someId);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();

		firstFactoredTableFacadeSpy.MCR.assertParameters("updateRowsUsingQuery", 0, tableQuerySpy);
	}

	@Test
	public void testUpdateParameterRecordNotValidJson() throws Exception {

		storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList, dataDivider);

		String dataRecordJson = getConvertedJson(dataRecord);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		tableQuerySpy.MCR.assertParameters("addParameter", 0, "datadivider", dataDivider);
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(dataRecordJson);

		PGobject jsonObject2 = (PGobject) tableQuerySpy.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("addParameter", 1, "value");
		assertEquals(jsonObject2.getType(), "json");
		assertEquals(jsonObject2.getValue(), dataRecordJson);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", someId);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();

		firstFactoredTableFacadeSpy.MCR.assertParameters("updateRowsUsingQuery", 0, tableQuerySpy);
	}

	@Test
	public void testUpdateTypeOrIdNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnUpdate = true;
		try {
			storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof StorageException);
			assertEquals(e.getMessage(),
					"Storage exception when updating record with recordType: someType with id: someId");
			assertEquals(e.getCause().getMessage(), "Error from updateRowsUsingQuery in tablespy");
		}
	}

	@Test
	public void testUpdateNoRecordUpdated() throws Exception {
		sqlDatabaseFactorySpy.numberOfAffectedRows = 0;

		try {
			storage.update(someType, someId, dataRecord, emptyStorageTerms, emptyLinkList,
					dataDivider);
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"Record not found when updating record with recordType: someType and id: someId");
		}
	}

	@Test
	public void testDeleteByTypeAndId() {
		storage.deleteByTypeAndId("someType", "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "record");
		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", someId);

		TableFacadeSpy firstFactoredTableFacadeSpy = getFirstFactoredTableFacadeSpy();

		firstFactoredTableFacadeSpy.MCR.assertParameters("deleteRowsForQuery", 0, tableQuerySpy);
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
					"Storage exception when deleting record with recordType: someType with id: someId");
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
					"Record not found when deleting record with recordType: someType and id: someId");
		}
	}

	@Test(expectedExceptions = NotImplementedException.class, expectedExceptionsMessageRegExp = ""
			+ "linksExistForRecord is not implemented")
	public void testLinksExistForRecord() {
		storage.linksExistForRecord("someType", "someId");
	}

	@Test(expectedExceptions = NotImplementedException.class, expectedExceptionsMessageRegExp = ""
			+ "readAbstractList is not implemented")
	public void testAbstractList() {
		storage.readAbstractList("someType", new DataGroupSpy());
	}

	@Test(expectedExceptions = NotImplementedException.class, expectedExceptionsMessageRegExp = ""
			+ "generateLinkCollectionPointingToRecord is not implemented")
	public void testGenerateLinkCollectionPointingToRecord() {
		storage.generateLinkCollectionPointingToRecord("someType", "someId");
	}

	@Test
	public void testRecordExists_notFound0() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 0;
		assertFalse(storage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId("someType",
				"someId"));
	}

	@Test
	public void testRecordExists_TableFacadeFactoredAndCloseCalled() throws Exception {
		assertFalse(storage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId("someType",
				"someId"));

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testRecordExists_NotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;
		try {
			assertFalse(storage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId(
					"someType", "someId"));
			makeSureErrorIsThrownFromAboveStatements();

		} catch (Exception e) {
			assertTrue(e instanceof RecordNotFoundException);
			assertEquals(e.getMessage(),
					"RecordType: someType, with id: someId, not found in storage.");
			assertEquals(e.getCause().getMessage(), "Error from readNumberOfRows in tablespy");
		}
	}

	@Test
	public void testRecordExists() throws Exception {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 1;

		boolean recordExists = storage
				.recordExistsForAbstractOrImplementingRecordTypeAndRecordId("someType", "someId");

		TableQuerySpy tableQuerySpy = getFactoredTableQueryUsingCallNumber(0);
		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();

		sqlDatabaseFactorySpy.MCR.assertParameter("factorTableQuery", 0, "tableName", "record");

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", "someId");

		tableFacadeSpy.MCR.assertParameters("readNumberOfRows", 0, tableQuerySpy);
		assertTrue(recordExists);
	}

	@Test
	public void testRecordExists_Found747() {
		sqlDatabaseFactorySpy.totalNumberOfRecordsForType = 747;
		assertTrue(storage.recordExistsForAbstractOrImplementingRecordTypeAndRecordId("someType",
				"someId"));
	}

	@Test(expectedExceptions = NotImplementedException.class, expectedExceptionsMessageRegExp = ""
			+ "getTotalNumberOfRecordsForAbstractType is not implemented")
	public void testGetTotalNumberOfRecordsForAbstractType() {
		storage.getTotalNumberOfRecordsForAbstractType("someAbstractType", Collections.emptyList(),
				new DataGroupSpy());
	}

}
