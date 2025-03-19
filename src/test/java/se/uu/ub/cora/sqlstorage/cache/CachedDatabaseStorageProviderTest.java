/*
 * Copyright 2021, 2022, 2025 Uppsala University Library
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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicstorage.RecordStorageInMemory;
import se.uu.ub.cora.data.DataProvider;
import se.uu.ub.cora.data.spies.DataFactorySpy;
import se.uu.ub.cora.data.spies.DataRecordGroupSpy;
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
import se.uu.ub.cora.storage.spies.RecordStorageSpy;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class CachedDatabaseStorageProviderTest {
	private static final String SOME_TYPE = "someType";
	private static final String SOME_ID = "someId";
	private Map<String, String> initInfo = new HashMap<>();
	private LoggerFactorySpy loggerFactorySpy;
	private OnlyForTestCachedDatabaseStorageInstanceProvider provider;
	private FromDbStoragePopulatorSpy populatorSpy;
	private RecordStorageSpy memoryStorageSpy;
	private RecordStorageSpy databaseStorageSpy;
	private DataFactorySpy dataFactory;
	private DataRecordGroupSpy dataRecordGroupSpy;

	@BeforeMethod
	public void beforeMethod() {
		setUpFactories();
		memoryStorageSpy = new RecordStorageSpy();
		databaseStorageSpy = setUpDatabaseStorageWithOneDataRecordGroupForRead();

		DatabaseStorageInstance.setInstance(null);
		setUpDefaultInitInfo();
		provider = new OnlyForTestCachedDatabaseStorageInstanceProvider();
		populatorSpy = new FromDbStoragePopulatorSpy();
	}

	private RecordStorageSpy setUpDatabaseStorageWithOneDataRecordGroupForRead() {
		RecordStorageSpy databaseStorageSpyInt = new RecordStorageSpy();
		dataRecordGroupSpy = new DataRecordGroupSpy();
		dataRecordGroupSpy.MRV.setDefaultReturnValuesSupplier("getDataDivider",
				() -> "someDataDivider");
		databaseStorageSpyInt.MRV.setDefaultReturnValuesSupplier("read", () -> dataRecordGroupSpy);
		return databaseStorageSpyInt;
	}

	private void setUpFactories() {
		loggerFactorySpy = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactorySpy);

		dataFactory = new DataFactorySpy();
		DataProvider.onlyForTestSetDataFactory(dataFactory);
	}

	@AfterMethod
	private void afterMethod() {
		LoggerProvider.setLoggerFactory(null);
		DataProvider.onlyForTestSetDataFactory(null);
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
	public void testDatabaseRecordStorageStartedWithSqlDatabaseFactory() {
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
	public void testDatabaseRecordStorageStartedWithJsonParser() {
		CachedDatabaseRecordStorage recordStorage = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		DatabaseRecordStorage database = (DatabaseRecordStorage) recordStorage
				.onlyForTestGetDatabase();
		JsonParser jsonParser = database.onlyForTestGetJsonParser();
		assertTrue(jsonParser instanceof OrgJsonParser);
	}

	@Test
	public void testLoggingNormalStartupCached() {
		provider.getRecordStorage();

		LoggerSpy logger = getLoggerSpy();
		logger.MCR.assertParameters("logInfoUsingMessage", 0,
				"CachedDatabaseStorageInstanceProvider starting...");
		logger.MCR.assertParameters("logInfoUsingMessage", 1,
				"starting in memory cached DatabaseRecordStorage");
		logger.MCR.assertParameters("logInfoUsingMessage", 2,
				"CachedDatabaseStorageInstanceProvider started");
	}

	@Test
	public void testLoggingNormalStartupDirectDtabase() {
		setDoNotCache();

		provider.getRecordStorage();

		LoggerSpy logger = getLoggerSpy();
		logger.MCR.assertParameters("logInfoUsingMessage", 0,
				"CachedDatabaseStorageInstanceProvider starting...");
		logger.MCR.assertParameters("logInfoUsingMessage", 1,
				"starting direct DatabaseRecordStorage");
		logger.MCR.assertParameters("logInfoUsingMessage", 2,
				"CachedDatabaseStorageInstanceProvider started");
	}

	private LoggerSpy getLoggerSpy() {
		loggerFactorySpy.MCR.assertParameters("factorForClass", 0,
				CachedDatabaseStorageInstanceProvider.class);
		return (LoggerSpy) loggerFactorySpy.MCR.getReturnValue("factorForClass", 0);
	}

	@Test(expectedExceptions = InitializationException.class)
	public void testErrorMissingNoInitInfo() {
		SettingsProvider.setSettings(null);
		provider.getRecordStorage();
	}

	@Test
	public void testOnlyOneInstance() {
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
	public void testOneStaticInstance() {
		CachedDatabaseRecordStorage recordStorage = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		provider = new OnlyForTestCachedDatabaseStorageInstanceProvider();
		CachedDatabaseRecordStorage recordStorage2 = (CachedDatabaseRecordStorage) provider
				.getRecordStorage();
		assertSame(recordStorage2, recordStorage);
	}

	@Test
	public void testAssertParametersPassedToPopulator() {
		provider.getRecordStorage();

		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) provider.MCR
				.getParameterForMethodAndCallNumberAndParameter("createPopulater", 0,
						"sqlDatabaseFactory");
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");

		OrgJsonParser jsonParser = (OrgJsonParser) provider.MCR
				.getParameterForMethodAndCallNumberAndParameter("createPopulater", 0, "jsonParser");
		assertTrue(jsonParser instanceof OrgJsonParser);

		var memory = populatorSpy.MCR.getParameterForMethodAndCallNumberAndParameter(
				"populateStorageFromDatabase", 0, "recordStorageInMemory");
		assertTrue(memory instanceof RecordStorageInMemory);
	}

	@Test
	public void testCreatePopulaterMethod() {
		SqlDatabaseFactorySpy sqlDatabaseFactory = new SqlDatabaseFactorySpy();
		JsonParserSpy jsonParser = new JsonParserSpy();
		FromDbStoragePopulatorImp populator = (FromDbStoragePopulatorImp) provider
				.callSuperCreatePopulaterAndReturnResult(sqlDatabaseFactory, jsonParser);

		assertSame(sqlDatabaseFactory.MCR.getReturnValue("factorDatabaseFacade", 0),
				populator.onlyForTestGetDatabaseFacade());
		assertSame(jsonParser, populator.onlyForTestGetJsonParser());
	}

	@Test
	public void testCreateNonCachedDbStorage() {
		setDoNotCache();

		DatabaseRecordStorage database = (DatabaseRecordStorage) provider.getRecordStorage();

		SqlDatabaseFactoryImp sqlDatabaseFactory = (SqlDatabaseFactoryImp) database
				.onlyForTestGetSqlDatabaseFactory();
		assertNotNull(sqlDatabaseFactory);
		String lookupName = sqlDatabaseFactory.onlyForTestGetLookupName();
		assertEquals(lookupName, "java:/comp/env/jdbc/coraPostgres");
	}

	@Test
	public void testCreateCachedDbStorageWith_doNotCache_setting() {
		setDoCache();

		RecordStorage storage = provider.getRecordStorage();

		assertTrue(storage instanceof CachedDatabaseRecordStorage);
	}

	private void setDoCache() {
		initInfo.put("doNotCache", "false");
	}

	private void setDoNotCache() {
		initInfo.put("doNotCache", "true");
	}

	@Test
	public void testDataChanged_createAction_withDoCache() {
		setDoCache();
		var spyProvider = new OnlyForTestCachedDatabaseStorageInstanceProvider2();
		spyProvider.getRecordStorage();

		spyProvider.dataChanged(SOME_TYPE, SOME_ID, "create");

		databaseStorageSpy.MCR.assertParameters("read", 0, SOME_TYPE, SOME_ID);
		var readRecord = databaseStorageSpy.MCR.getReturnValue("read", 0);
		var dataGroup = dataFactory.MCR
				.assertCalledParametersReturn("factorGroupFromDataRecordGroup", readRecord);
		var storageTerms = databaseStorageSpy.MCR
				.assertCalledParametersReturn("getStorageTermsForRecord", SOME_TYPE, SOME_ID);
		var links = databaseStorageSpy.MCR.assertCalledParametersReturn("getLinksFromRecord",
				SOME_TYPE, SOME_ID);
		memoryStorageSpy.MCR.assertParameters("create", 0, SOME_TYPE, SOME_ID, dataGroup,
				storageTerms, links, "someDataDivider");
		memoryStorageSpy.MCR.assertMethodNotCalled("update");
		memoryStorageSpy.MCR.assertMethodNotCalled("deleteByTypeAndId");
	}

	@Test
	public void testDataChanged_updateAction_withDoCache() {
		setDoCache();
		var spyProvider = new OnlyForTestCachedDatabaseStorageInstanceProvider2();
		spyProvider.getRecordStorage();

		spyProvider.dataChanged(SOME_TYPE, SOME_ID, "update");

		databaseStorageSpy.MCR.assertParameters("read", 0, SOME_TYPE, SOME_ID);
		var readRecord = databaseStorageSpy.MCR.getReturnValue("read", 0);
		var dataGroup = dataFactory.MCR
				.assertCalledParametersReturn("factorGroupFromDataRecordGroup", readRecord);
		var storageTerms = databaseStorageSpy.MCR
				.assertCalledParametersReturn("getStorageTermsForRecord", SOME_TYPE, SOME_ID);
		var links = databaseStorageSpy.MCR.assertCalledParametersReturn("getLinksFromRecord",
				SOME_TYPE, SOME_ID);
		memoryStorageSpy.MCR.assertParameters("update", 0, SOME_TYPE, SOME_ID, dataGroup,
				storageTerms, links, "someDataDivider");
		memoryStorageSpy.MCR.assertMethodNotCalled("create");
		memoryStorageSpy.MCR.assertMethodNotCalled("deleteByTypeAndId");
	}

	@Test
	public void testDataChanged_deleteAction_withDoCache() {
		setDoCache();
		var spyProvider = new OnlyForTestCachedDatabaseStorageInstanceProvider2();
		spyProvider.getRecordStorage();

		spyProvider.dataChanged(SOME_TYPE, SOME_ID, "delete");

		memoryStorageSpy.MCR.assertParameters("deleteByTypeAndId", 0, SOME_TYPE, SOME_ID);

		databaseStorageSpy.MCR.assertMethodNotCalled("read");
		memoryStorageSpy.MCR.assertMethodNotCalled("create");
		memoryStorageSpy.MCR.assertMethodNotCalled("update");
	}

	@Test
	public void testDataChanged_withDoNotCache() {
		setDoNotCache();
		var spyProvider = new OnlyForTestCachedDatabaseStorageInstanceProvider2();
		spyProvider.getRecordStorage();

		spyProvider.dataChanged(SOME_TYPE, SOME_ID, "create");
		spyProvider.dataChanged(SOME_TYPE, SOME_ID, "update");
		spyProvider.dataChanged(SOME_TYPE, SOME_ID, "delete");

		assertCacheIsNotUpdated();
	}

	private void assertCacheIsNotUpdated() {
		databaseStorageSpy.MCR.assertMethodNotCalled("read");
		memoryStorageSpy.MCR.assertMethodNotCalled("create");
		memoryStorageSpy.MCR.assertMethodNotCalled("update");
		memoryStorageSpy.MCR.assertMethodNotCalled("deleteByTypeAndId");
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

	private class OnlyForTestCachedDatabaseStorageInstanceProvider2
			extends CachedDatabaseStorageInstanceProvider {

		public MethodCallRecorder MCR = new MethodCallRecorder();
		public MethodReturnValues MRV = new MethodReturnValues();

		public OnlyForTestCachedDatabaseStorageInstanceProvider2() {
			MCR.useMRV(MRV);
			MRV.setDefaultReturnValuesSupplier("createPopulater", () -> populatorSpy);
			MRV.setDefaultReturnValuesSupplier("createDatabaseRecordStorage",
					() -> databaseStorageSpy);
			MRV.setDefaultReturnValuesSupplier("createRecordStorageInMemory",
					() -> memoryStorageSpy);
		}

		@Override
		protected FromDbStoragePopulator createPopulater(SqlDatabaseFactory sqlDatabaseFactory,
				JsonParser jsonParser) {
			return (FromDbStoragePopulator) MCR.addCallAndReturnFromMRV("sqlDatabaseFactory",
					sqlDatabaseFactory, "jsonParser", jsonParser);
		}

		@Override
		protected RecordStorage createDatabaseRecordStorage(SqlDatabaseFactory sqlDatabaseFactory,
				JsonParser jsonParser) {
			return (RecordStorage) MCR.addCallAndReturnFromMRV("sqlDatabaseFactory",
					sqlDatabaseFactory, "jsonParser", jsonParser);
		}

		@Override
		protected RecordStorage createRecordStorageInMemory() {
			return (RecordStorage) MCR.addCallAndReturnFromMRV();
		}

	}
}
