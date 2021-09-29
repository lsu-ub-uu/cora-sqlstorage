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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageReadResult;

public class DatabaseRecordStorageTest {

	private RecordStorage storage;
	private SqlDatabaseFactorySpy sqlDatabaseFactorySpy;
	private JsonParserSpy jsonParserSpy;
	private JsonToDataConverterFactorySpy factoryCreatorSpy;
	private DataGroup emptyFilterSpy;
	private FilterDataGroupSpy filterSpy;

	@BeforeMethod
	public void beforeMethod() {
		filterSpy = new FilterDataGroupSpy();
		emptyFilterSpy = new DataGroupSpy();
		factoryCreatorSpy = new JsonToDataConverterFactorySpy();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(factoryCreatorSpy);
		sqlDatabaseFactorySpy = new SqlDatabaseFactorySpy();
		jsonParserSpy = new JsonParserSpy();
		storage = new DatabaseRecordStorage(sqlDatabaseFactorySpy, jsonParserSpy);
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

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "someType");
		TableQuerySpy tableQuerySpy = getFirstFactoredTableQuery();

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", "someId");

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readOneRowForQuery", 0, tableQuerySpy);
	}

	private TableQuerySpy getFirstFactoredTableQuery() {
		return (TableQuerySpy) sqlDatabaseFactorySpy.MCR.getReturnValue("factorTableQuery", 0);
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
			assertEquals(e.getMessage(), "No records found for recordType: someType");
			assertEquals(e.getCause().getMessage(), "Error from readRowsForQuery in tablespy");
		}
	}

	private void makeSureErrorIsThrownFromAboveStatements() {
		assertTrue(false);
	}

	@Test
	public void testReadListTableQueryFactoredAndTableFacadeCalled() throws Exception {

		storage.readList("someType", emptyFilterSpy);

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "someType");
		TableQuerySpy tableQuerySpy = getFirstFactoredTableQuery();

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readRowsForQuery", 0, tableQuerySpy);

	}

	@Test
	public void testReadListReturnsAStorageReadResult() throws Exception {

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
		readRow.MCR.assertParameters("getValueByColumn", 0, "dataRecord");
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

		TableQuerySpy tableQuerySpy = getFirstFactoredTableQuery();

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 1L);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 10L);

	}

	@Test
	public void testReadListWithFromNoAndToNoInFilterHigher() throws Exception {
		filterSpy.fromNo = "10";
		filterSpy.toNo = "100";
		storage.readList("someType", filterSpy);

		TableQuerySpy tableQuerySpy = getFirstFactoredTableQuery();

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 10L);
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 100L);
	}

	@Test
	public void testReadListWithFromNoInFilter() throws Exception {
		filterSpy.fromNo = "10";
		storage.readList("someType", filterSpy);

		TableQuerySpy tableQuerySpy = getFirstFactoredTableQuery();

		tableQuerySpy.MCR.assertParameters("setFromNo", 0, 10L);
		tableQuerySpy.MCR.assertMethodNotCalled("setToNo");
	}

	@Test
	public void testReadListWithToNoInFilter() throws Exception {
		filterSpy.toNo = "100";
		storage.readList("someType", filterSpy);

		TableQuerySpy tableQuerySpy = getFirstFactoredTableQuery();

		tableQuerySpy.MCR.assertMethodNotCalled("setFromNo");
		tableQuerySpy.MCR.assertParameters("setToNo", 0, 100L);
	}
}
