/*
 * Copyright 2021,2022 Uppsala University Library
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

import se.uu.ub.cora.basicstorage.RecordStorageInMemory;
import se.uu.ub.cora.initialize.InitializationException;
import se.uu.ub.cora.initialize.SettingsProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.logger.Logger;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.internal.DatabaseRecordStorage;
import se.uu.ub.cora.sqlstorage.internal.DatabaseStorageInstance;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.RecordStorageInstanceProvider;

public class CachedDatabaseStorageInstanceProvider implements RecordStorageInstanceProvider {

	private Logger log = LoggerProvider
			.getLoggerForClass(CachedDatabaseStorageInstanceProvider.class);
	private static final String LOOKUP_NAME = "coraDatabaseLookupName";
	private String databaseLookupValue;

	@Override
	public int getOrderToSelectImplementionsBy() {
		return 10;
	}

	@Override
	public RecordStorage getRecordStorage() {
		possiblyStartStorage();
		return DatabaseStorageInstance.getInstance();
	}

	static void setStaticInstance(RecordStorage recordStorage) {
		DatabaseStorageInstance.setInstance(recordStorage);
	}

	private synchronized void possiblyStartStorage() {
		if (storageNotStarted()) {
			logAndStartStorage();
		}
	}

	private boolean storageNotStarted() {
		return DatabaseStorageInstance.getInstance() == null;
	}

	private void logAndStartStorage() {
		log.logInfoUsingMessage(
				"CachedDatabaseStorageInstanceProvider starting DatabaseCachedRecordStorage...");
		startStorage();
		log.logInfoUsingMessage(
				"CachedDatabaseStorageInstanceProvider started DatabaseCachedRecordStorage");
	}

	private void startStorage() {
		databaseLookupValue = SettingsProvider.getSetting(LOOKUP_NAME);
		createDependenciesAndStartStorage();
	}

	private void createDependenciesAndStartStorage() {
		RecordStorage cachedDbStorage = startCachedDbStorage();
		setStaticInstance(cachedDbStorage);
	}

	private RecordStorage startCachedDbStorage() {
		SqlDatabaseFactory sqlDatabaseFactory = SqlDatabaseFactoryImp
				.usingLookupNameFromContext(databaseLookupValue);
		JsonParser jsonParser = new OrgJsonParser();
		DatabaseRecordStorage database = new DatabaseRecordStorage(sqlDatabaseFactory, jsonParser);
		if (shouldNotCache()) {
			return database;
		}
		RecordStorageInMemory memory = new RecordStorageInMemory();
		return populateFromDatabase(sqlDatabaseFactory, jsonParser, database, memory);
	}

	private boolean shouldNotCache() {
		try {
			String setting = SettingsProvider.getSetting("doNotCache");
			return "true".equals(setting);
		} catch (InitializationException e) {
			return false;
		}
	}

	private CachedDatabaseRecordStorage populateFromDatabase(SqlDatabaseFactory sqlDatabaseFactory,
			JsonParser jsonParser, DatabaseRecordStorage database, RecordStorageInMemory memory) {
		FromDbStoragePopulator populator = createPopulater(sqlDatabaseFactory, jsonParser);
		populator.populateStorageFromDatabase(memory);
		return CachedDatabaseRecordStorage.usingDatabaseAndMemory(database, memory);
	}

	protected FromDbStoragePopulator createPopulater(SqlDatabaseFactory sqlDatabaseFactory,
			JsonParser jsonParser) {
		return new FromDbStoragePopulatorImp(sqlDatabaseFactory.factorDatabaseFacade(), jsonParser);
	}
}
