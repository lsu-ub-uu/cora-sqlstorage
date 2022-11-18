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
import se.uu.ub.cora.logger.spies.LoggerFactorySpy;
import se.uu.ub.cora.logger.spies.LoggerSpy;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.internal.DatabaseStorageInstance;
import se.uu.ub.cora.storage.RecordStorage;

public class DatabaseStorageProviderTest {
	private Map<String, String> initInfo = new HashMap<>();
	private LoggerFactorySpy loggerFactorySpy;
	private DatabaseStorageInstanceProvider provider;

	@BeforeMethod
	public void beforeMethod() {
		setUpFactories();
		DatabaseStorageInstance.setInstance(null);
		setUpDefaultInitInfo();
		provider = new DatabaseStorageInstanceProvider();
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
		assertEquals(provider.getOrderToSelectImplementionsBy(), 1000);
	}

	@Test
	public void testNormalStartupReturnsDatabaseRecordStorage() {
		RecordStorage recordStorage = provider.getRecordStorage();

		assertTrue(recordStorage instanceof DatabaseRecordStorage);
	}

	@Test
	public void testDatabaseRecordStorageStartedWithSqlDatabaseFactory() throws Exception {
		DatabaseRecordStorage recordStorage = (DatabaseRecordStorage) provider.getRecordStorage();
		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) recordStorage
				.onlyForTestGetSqlDatabaseFactory();
		assertNotNull(sqlDatabaseFactory);
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");
	}

	@Test
	public void testDatabaseRecordStorageStartedWithJsonParser() throws Exception {
		DatabaseRecordStorage recordStorage = (DatabaseRecordStorage) provider.getRecordStorage();
		JsonParser jsonParser = recordStorage.onlyForTestGetJsonParser();
		assertTrue(jsonParser instanceof OrgJsonParser);
	}

	@Test
	public void testLoggingNormalStartup() {
		provider.getRecordStorage();

		LoggerSpy logger = getLoggerSpy();
		logger.MCR.assertParameters("logInfoUsingMessage", 0,
				"DatabaseStorageInstanceProvider starting DatabaseRecordStorage...");
		logger.MCR.assertParameters("logInfoUsingMessage", 1,
				"DatabaseStorageInstanceProvider started DatabaseRecordStorage");
	}

	private LoggerSpy getLoggerSpy() {
		loggerFactorySpy.MCR.assertParameters("factorForClass", 0,
				DatabaseStorageInstanceProvider.class);
		LoggerSpy logger = (LoggerSpy) loggerFactorySpy.MCR.getReturnValue("factorForClass", 0);
		return logger;
	}

	@Test(expectedExceptions = InitializationException.class)
	public void testErrorMissingNoInitInfo() {
		SettingsProvider.setSettings(null);
		provider.getRecordStorage();
	}

	@Test
	public void testOnlyOneInstance() throws Exception {
		DatabaseRecordStorage recordStorage = (DatabaseRecordStorage) provider.getRecordStorage();
		DatabaseRecordStorage recordStorage2 = (DatabaseRecordStorage) provider.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}

	@Test
	public void testThreadsWhenCreatingConnectionProvider() throws Exception {
		Class<?>[] methodParameters = {};
		Method declaredMethod = DatabaseStorageInstanceProvider.class
				.getDeclaredMethod("possiblyStartStorage", methodParameters);
		assertTrue(Modifier.isSynchronized(declaredMethod.getModifiers()));
	}

	@Test
	public void testOneStaticInstance() throws Exception {
		DatabaseRecordStorage recordStorage = (DatabaseRecordStorage) provider.getRecordStorage();
		provider = new DatabaseStorageInstanceProvider();
		DatabaseRecordStorage recordStorage2 = (DatabaseRecordStorage) provider.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}
}
