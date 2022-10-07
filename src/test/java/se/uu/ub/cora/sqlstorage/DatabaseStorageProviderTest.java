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

import se.uu.ub.cora.initialize.InitializationException;
import se.uu.ub.cora.initialize.SettingsProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.internal.DatabaseStorageInstance;
import se.uu.ub.cora.sqlstorage.spy.log.LoggerFactorySpy;
import se.uu.ub.cora.storage.RecordStorage;

public class DatabaseStorageProviderTest {
	private Map<String, String> initInfo = new HashMap<>();
	private Map<String, String> emptyInitInfo = new HashMap<>();
	private LoggerFactorySpy loggerFactorySpy;
	private String testedClassName = "DatabaseStorageProvider";
	private DatabaseStorageProvider provider;

	@BeforeMethod
	public void beforeMethod() {
		DatabaseStorageInstance.setInstance(null);
		setUpFactories();
		setUpDefaultInitInfo();
		provider = new DatabaseStorageProvider();
	}

	private void setUpFactories() {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);
	}

	private void setUpDefaultInitInfo() {
		initInfo = new HashMap<>();
		initInfo.put("coraDatabaseLookupName", "java:/comp/env/jdbc/coraPostgres");
		SettingsProvider.setSettings(initInfo);
	}

	@Test
	public void testGetOrderToSelectImplementationsByIsTen() {
		assertEquals(provider.getOrderToSelectImplementionsBy(), 10);
	}

	@Test
	public void testNormalStartupReturnsDatabaseRecordStorage() {
		RecordStorage recordStorage = provider.getRecordStorage();

		assertTrue(recordStorage instanceof DatabaseRecordStorage);
	}

	@Test
	public void testDatabaseRecordStorageStartedWithSqlDatabaseFactory() throws Exception {
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) recordStorage
				.onlyForTestGetSqlDatabaseFactory();
		assertNotNull(sqlDatabaseFactory);
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");
	}

	@Test
	public void testDatabaseRecordStorageStartedWithJsonParser() throws Exception {
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		JsonParser jsonParser = recordStorage.onlyForTestGetJsonParser();
		assertTrue(jsonParser instanceof OrgJsonParser);
	}

	@Test
	public void testLoggingNormalStartup() {
		provider.getRecordStorage();

		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"DatabaseStorageProvider starting DatabaseRecordStorage...");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 1),
				"Found java:/comp/env/jdbc/coraPostgres as coraDatabaseLookupName");
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 2),
				"DatabaseStorageProvider started DatabaseRecordStorage");
	}

	@Test(expectedExceptions = InitializationException.class)
	public void testErrorMissingLookupNameInInitInfo() {
		SettingsProvider.setSettings(null);
		provider.getRecordStorage();
	}

	@Test
	public void testLoggingMissingLookupNameInInitInfo() throws Exception {
		try {
			SettingsProvider.setSettings(emptyInitInfo);
			provider.getRecordStorage();
			assertTrue(false);
		} catch (Exception e) {
		}
		assertEquals(loggerFactorySpy.getInfoLogMessageUsingClassNameAndNo(testedClassName, 0),
				"DatabaseStorageProvider starting DatabaseRecordStorage...");
		System.out.println(loggerFactorySpy.createdLoggers.toString());

	}

	@Test
	public void testOnlyOneInstance() throws Exception {
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		DatabaseRecordStorage recordStorage2 = provider.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}

	@Test
	public void testThreadsWhenCreatingConnectionProvider() throws Exception {
		Class<?>[] methodParameters = {};
		Method declaredMethod = DatabaseStorageProvider.class
				.getDeclaredMethod("possiblyStartStorage", methodParameters);
		assertTrue(Modifier.isSynchronized(declaredMethod.getModifiers()));
	}

	@Test
	public void testOneStaticInstance() throws Exception {
		DatabaseRecordStorage recordStorage = provider.getRecordStorage();
		provider = new DatabaseStorageProvider();
		DatabaseRecordStorage recordStorage2 = provider.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}
}
