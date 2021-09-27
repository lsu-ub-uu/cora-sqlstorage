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

import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;

public class DatabaseRecordStorageTest {

	private RecordStorage storage;
	private SqlDatabaseFactorySpy sqlDatabaseFactorySpy;

	@BeforeMethod
	public void beforeMethod() {
		sqlDatabaseFactorySpy = new SqlDatabaseFactorySpy();
		storage = new DatabaseRecordStorage(sqlDatabaseFactorySpy);
	}

	@Test
	public void testReadTableFacadeFactoredAndCloseCalled() throws Exception {
		storage.read("someType", "someId");
		TableFacadeSpy tableFacadeSpy = (TableFacadeSpy) sqlDatabaseFactorySpy.MCR
				.getReturnValue("factorTableFacade", 0);
		tableFacadeSpy.MCR.assertMethodWasCalled("close");
	}

	@Test
	public void testReadParametersAddedToTableQueryAndPassedOn() throws Exception {
		storage.read("someType", "someId");

		sqlDatabaseFactorySpy.MCR.assertParameters("factorTableQuery", 0, "someType");
		TableQuerySpy tableQuerySpy = (TableQuerySpy) sqlDatabaseFactorySpy.MCR
				.getReturnValue("factorTableQuery", 0);

		tableQuerySpy.MCR.assertParameters("addCondition", 0, "id", "someId");

		TableFacadeSpy tableFacadeSpy = (TableFacadeSpy) sqlDatabaseFactorySpy.MCR
				.getReturnValue("factorTableFacade", 0);
		tableFacadeSpy.MCR.assertParameters("readOneRowForQuery", 0, tableQuerySpy);
	}

	@Test(expectedExceptions = RecordNotFoundException.class, expectedExceptionsMessageRegExp = ""
			+ "No record found for recordType: nonExistingRecordType with id: someId")
	public void testReadTypeNotFound() throws Exception {
		sqlDatabaseFactorySpy.throwExceptionOnTableFacadeOnRead = true;

		storage.read("nonExistingRecordType", "someId");
	}

	// @Test(expectedExceptions = RecordNotFoundException.class, expectedExceptionsMessageRegExp =
	// ""
	// + "No record found for recordType: existingType with id: someId")
	// public void testReadIdNotFound() throws Exception {
	// readerFactory.throwExceptionOnRecordReaderOnRead = true;
	//
	// storage.read("existingType", "someId");
	// }
	//
	// // Timeout
	// // Lost connection
	//
	// @Test
	// public void testReadOkConditionsSentToRecordReader() {
	// String recordType = "someRecordType";
	// String id = "someId";
	//
	// storage.read(recordType, id);
	//
	// TableFacadeSpy recordReader = (TableFacadeSpy) readerFactory.MCR.getReturnValue("factor",
	// 0);
	// recordReader.MCR.assertParameter("readOneRowFromDbUsingTableAndConditions", 0, "tableName",
	// recordType);
	//
	// Map<String, Object> conditions = (Map<String, Object>) recordReader.MCR
	// .getValueForMethodNameAndCallNumberAndParameterName(
	// "readOneRowFromDbUsingTableAndConditions", 0, "conditions");
	// assertEquals(conditions.size(), 1);
	// assertEquals(conditions.get("id"), id);
	//
	// // TODO: check convertions and return... but first fix other cases...
	//
	// }
	//
	// @Test
	// public void testReadOkReadJsonConvertedToDataGroup() throws Exception {
	// String recordType = "someRecordType";
	// String id = "someId";
	//
	// storage.read(recordType, id);
	//
	// }

}
