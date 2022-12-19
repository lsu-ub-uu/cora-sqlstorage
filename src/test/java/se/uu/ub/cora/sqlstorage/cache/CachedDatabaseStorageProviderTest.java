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
package se.uu.ub.cora.sqlstorage.cache;

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

import se.uu.ub.cora.basicstorage.RecordStorageInMemory;
import se.uu.ub.cora.initialize.InitializationException;
import se.uu.ub.cora.initialize.SettingsProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.logger.spies.LoggerFactorySpy;
import se.uu.ub.cora.logger.spies.LoggerSpy;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.DatabaseStorageInstanceProvider;
import se.uu.ub.cora.sqlstorage.internal.DatabaseRecordStorage;
import se.uu.ub.cora.sqlstorage.internal.DatabaseStorageInstance;
import se.uu.ub.cora.sqlstorage.spy.json.JsonParserSpy;
import se.uu.ub.cora.sqlstorage.spy.sql.SqlDatabaseFactorySpy;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class CachedDatabaseStorageProviderTest {
	private Map<String, String> initInfo = new HashMap<>();
	private LoggerFactorySpy loggerFactorySpy;
	private OnlyForTestCachedDatabaseStorageInstanceProvider provider;
	private FromDbStoragePopulatorSpy populatorSpy;

	@BeforeMethod
	public void beforeMethod() {
		setUpFactories();
		DatabaseStorageInstance.setInstance(null);
		setUpDefaultInitInfo();
		provider = new OnlyForTestCachedDatabaseStorageInstanceProvider();
		populatorSpy = new FromDbStoragePopulatorSpy();
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

		assertTrue(recordStorage instanceof CachedDatabaseRecordStorage);
	}

	@Test
	public void testDatabaseRecordStorageStartedWithSqlDatabaseFactory() throws Exception {
		CachedDatabaseRecordStorage recordStorage = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		DatabaseRecordStorage database = (DatabaseRecordStorage) recordStorage
				.onlyForTestGetDatabase();
		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) database
				.onlyForTestGetSqlDatabaseFactory();
		assertNotNull(sqlDatabaseFactory);
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");
	}

	@Test
	public void testDatabaseRecordStorageStartedWithJsonParser() throws Exception {
		CachedDatabaseRecordStorage recordStorage = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		DatabaseRecordStorage database = (DatabaseRecordStorage) recordStorage
				.onlyForTestGetDatabase();
		JsonParser jsonParser = database.onlyForTestGetJsonParser();
		assertTrue(jsonParser instanceof OrgJsonParser);
	}

	@Test
	public void testLoggingNormalStartup() {
		provider.getRecordStorage();

		LoggerSpy logger = getLoggerSpy();
		logger.MCR.assertParameters("logInfoUsingMessage", 0,
				"CachedDatabaseStorageInstanceProvider starting DatabaseCachedRecordStorage...");
		logger.MCR.assertParameters("logInfoUsingMessage", 1,
				"CachedDatabaseStorageInstanceProvider started DatabaseCachedRecordStorage");
	}

	private LoggerSpy getLoggerSpy() {
		loggerFactorySpy.MCR.assertParameters("factorForClass", 0,
				CachedDatabaseStorageInstanceProvider.class);
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
		CachedDatabaseRecordStorage recordStorage = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		CachedDatabaseRecordStorage recordStorage2 = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
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
		CachedDatabaseRecordStorage recordStorage = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		provider = new OnlyForTestCachedDatabaseStorageInstanceProvider();
		CachedDatabaseRecordStorage recordStorage2 = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}

	@Test
	public void testAssertParametersPassedToPopulator() throws Exception {
		provider.getRecordStorage();

		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) provider.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("createPopulater", 0,
						"sqlDatabaseFactory");
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");

		OrgJsonParser jsonParser = (OrgJsonParser) provider.MCR
				.getValueForMethodNameAndCallNumberAndParameterName("createPopulater", 0,
						"jsonParser");
		assertTrue(jsonParser instanceof OrgJsonParser);

		var memory = populatorSpy.MCR.getValueForMethodNameAndCallNumberAndParameterName(
				"populateStorageFromDatabase", 0, "recordStorageInMemory");
		assertTrue(memory instanceof RecordStorageInMemory);
	}

	@Test
	public void testCreatePopulaterMethod() throws Exception {
		SqlDatabaseFactorySpy sqlDatabaseFactory = new SqlDatabaseFactorySpy();
		JsonParserSpy jsonParser = new JsonParserSpy();
		FromDbStoragePopulatorImp populator = (FromDbStoragePopulatorImp) provider
				.callSuperCreatePopulaterAndReturnResult(sqlDatabaseFactory, jsonParser);

		assertSame(sqlDatabaseFactory.MCR.getReturnValue("factorDatabaseFacade", 0),
				populator.onlyForTestGetDatabaseFacade());
		assertSame(jsonParser, populator.onlyForTestGetJsonParser());
	}

	@Test
	public void testCreateNonCachedDbStorage() throws Exception {
		initInfo.put("doNotCache", "true");

		DatabaseRecordStorage database = (DatabaseRecordStorage) provider.getRecordStorage();

		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) database
				.onlyForTestGetSqlDatabaseFactory();
		assertNotNull(sqlDatabaseFactory);
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");
	}

	@Test
	public void testCreateCachedDbStorageWith_doNotCache_setting() throws Exception {
		initInfo.put("doNotCache", "false");

		RecordStorage storage = provider.getRecordStorage();

		assertTrue(storage instanceof CachedDatabaseRecordStorage);
	}

	private class OnlyForTestCachedDatabaseStorageInstanceProvider
			extends CachedDatabaseStorageInstanceProvider {

		MethodCallRecorder MCR = new MethodCallRecorder();

		@Override
		protected FromDbStoragePopulator createPopulater(SqlDatabaseFactory sqlDatabaseFactory,
				JsonParser jsonParser) {
			MCR.addCall("sqlDatabaseFactory", sqlDatabaseFactory, "jsonParser", jsonParser);

			return populatorSpy;
		}

		protected FromDbStoragePopulator callSuperCreatePopulaterAndReturnResult(
				SqlDatabaseFactory sqlDatabaseFactory, JsonParser jsonParser) {
			return super.createPopulater(sqlDatabaseFactory, jsonParser);
		}

	}
}
