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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.internal.DatabaseRecordStorageInstance;
import se.uu.ub.cora.sqlstorage.log.LoggerFactorySpy;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageException;

public class DatabaseStorageProviderTest {
	private Map<String, String> initInfo = new HashMap<>();
	private Map<String, String> emptyInitInfo = new HashMap<>();
	private LoggerFactorySpy loggerFactorySpy;
	private String testedClassName = "DatabaseStorageProvider";
	private DatabaseStorageProvider provider;

	@BeforeMethod
	public void beforeMethod() {
		setUpFactories();
		setUpDefaultInitInfo();
		provider = new DatabaseStorageProvider();
		DatabaseRecordStorageInstance.setInstance(null);
	}

	private void setUpFactories() {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);
	}

	private void setUpDefaultInitInfo() {
		initInfo = new HashMap<>();
		initInfo.put("coraDatabaseLookupName", "java:/comp/env/jdbc/coraPostgres");
	}

	@Test
	public void testGetOrderToSelectImplementationsByIsOne() {
		provider.startUsingInitInfo(initInfo);
		assertEquals(provider.getOrderToSelectImplementionsBy(), 0);
	}

	@Test
	public void testNormalStartupReturnsDatabaseRecordStorage() {
		provider.startUsingInitInfo(initInfo);
		RecordStorage recordStorage = provider.getRecordStorage();
		assertTrue(recordStorage instanceof DatabaseRecordStorage);
	}

	@Test
	public void testDatabaseRecordStorageStartedWithSqlDatabaseFactory() throws Exception {
		provider.startUsingInitInfo(initInfo);
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) recordStorage
				.onlyForTestGetSqlDatabaseFactory();
		assertNotNull(sqlDatabaseFactory);
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "coraDatabaseLookupName");
	}

	@Test
	public void testDatabaseRecordStorageStartedWithJsonParser() throws Exception {
		provider.startUsingInitInfo(initInfo);
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		JsonParser jsonParser = recordStorage.onlyForTestGetJsonParser();
		assertTrue(jsonParser instanceof OrgJsonParser);
	}

	@Test
	public void testLoggingNormalStartup() {
		provider.startUsingInitInfo(initInfo);
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"DatabaseStorageProvider starting DatabaseRecordStorage...");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 1),
				"Found java:/comp/env/jdbc/coraPostgres as coraDatabaseLookupName");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 2),
				"DatabaseStorageProvider started DatabaseRecordStorage");
	}

	@Test(expectedExceptions = StorageException.class, expectedExceptionsMessageRegExp = ""
			+ "InitInfo must contain coraDatabaseLookupName")
	public void testErrorMissingLookupNameInInitInfo() throws Exception {
		provider.startUsingInitInfo(emptyInitInfo);
	}

	@Test
	public void testLoggingMissingLookupNameInInitInfo() throws Exception {
		try {
			provider.startUsingInitInfo(emptyInitInfo);
			assertTrue(false);
		} catch (Exception e) {
		}
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"DatabaseStorageProvider starting DatabaseRecordStorage...");
		assertEquals(loggerFactorySpy.getFatalLogMessageUsingClassNameAndNo(testedClassName, 0),
				"InitInfo must contain coraDatabaseLookupName");
	}

	@Test
	public void testOnlyOneInstance() throws Exception {
		provider.startUsingInitInfo(initInfo);
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		DatabaseRecordStorage recordStorage2 = provider.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}

	@Test
	public void testThreadsWhenCreatingConnectionProvider() throws Exception {
		Method declaredMethod = DatabaseStorageProvider.class.getDeclaredMethod("getRecordStorage");
		assertTrue(Modifier.isSynchronized(declaredMethod.getModifiers()));
	}

	@Test(expectedExceptions = StorageException.class, expectedExceptionsMessageRegExp = ""
			+ "DatabaseStorageProvider not started, please call startUsingInitInfo first.")
	public void testNotStarted() throws Exception {
		provider.getRecordStorage();
	}

	@Test
	public void testOneStaticInstance() throws Exception {
		provider.startUsingInitInfo(initInfo);
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		provider = new DatabaseStorageProvider();
		DatabaseRecordStorage recordStorage2 = provider.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}

	@Test
	public void testLogWhenAlreadyStarted() throws Exception {
		provider.startUsingInitInfo(initInfo);
		provider = new DatabaseStorageProvider();
		provider.startUsingInitInfo(initInfo);

		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"DatabaseRecordStorage already started, using that instance.");
	}

}
