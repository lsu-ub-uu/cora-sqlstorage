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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;

public class DatabaseRecordStorageTest {

	private RecordStorage storage;
	private SqlDatabaseFactorySpy sqlDatabaseFactorySpy;
	private JsonParserSpy jsonParserSpy;
	private JsonToDataConverterFactorySpy factoryCreatorSpy;

	@BeforeMethod
	public void beforeMethod() {

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
		TableQuerySpy tableQuerySpy = (TableQuerySpy) sqlDatabaseFactorySpy.MCR
				.getReturnValue("factorTableQuery", 0);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", "someId");

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertParameters("readOneRowForQuery", 0, tableQuerySpy);
	}

	private TableFacadeSpy getFirstFactoredTableFacadeSpy() {
		return (TableFacadeSpy) sqlDatabaseFactorySpy.MCR.getReturnValue("factorTableFacade", 0);
	}

	@Test(expectedExceptions = RecordNotFoundException.class, expectedExceptionsMessageRegExp = ""
			+ "No record found for recordType: someType with id: someId")
	public void testReadTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionFromTableFacadeOnRead = true;

		storage.read("someType", "someId");
	}

	@Test
	public void testReadOkReadJsonConvertedToDataGroup() throws Exception {
		String recordType = "someRecordType";
		String id = "someId";

		DataGroup readValueFromStorage = storage.read(recordType, id);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		RowSpy readRow = (RowSpy) tableFacadeSpy.MCR.getReturnValue("readOneRowForQuery", 0);

		readRow.MCR.assertParameters("getValueByColumn", 0, "dataRecord");

		Object dataRecord = readRow.MCR.getReturnValue("getValueByColumn", 0);

		// go on...
		jsonParserSpy.MCR.assertParameters("parseString", 0, dataRecord);
		JsonValue jsonValue = (JsonValue) jsonParserSpy.MCR.getReturnValue("parseString", 0);

		factoryCreatorSpy.MCR.assertParameters("createForJsonObject", 0, jsonValue);
		JsonToDataConverterSpy jsonToDataConverterSpy = (JsonToDataConverterSpy) factoryCreatorSpy.MCR
				.getReturnValue("createForJsonObject", 0);

		jsonToDataConverterSpy.MCR.assertReturn("toInstance", 0, readValueFromStorage);
	}

	@Test
	public void testReadListTableFacadeFactoredAndCloseCalled() throws Exception {
		DataGroup filterSpy = new DataGroupSpy();

		storage.readList("someType", filterSpy);

		TableFacadeSpy tableFacadeSpy = getFirstFactoredTableFacadeSpy();
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}
}
