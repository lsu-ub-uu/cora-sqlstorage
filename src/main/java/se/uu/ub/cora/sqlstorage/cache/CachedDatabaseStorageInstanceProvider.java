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

import java.util.Set;

import se.uu.ub.cora.basicstorage.RecordStorageInMemory;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataProvider;
import se.uu.ub.cora.data.DataRecordGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
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
	private boolean doNotCacheValue;
	private RecordStorage database;
	private RecordStorage memory;

	@Override
	public int getOrderToSelectImplementionsBy() {
		return 10;
	}

	@Override
	public RecordStorage getRecordStorage() {
		possiblyStartStorage();
		return DatabaseStorageInstance.getInstance();
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
		log.logInfoUsingMessage("CachedDatabaseStorageInstanceProvider starting...");
		try {
			startStorage();
		} catch (RuntimeException e) {
			log.logFatalUsingMessageAndException(
					"CachedDatabaseStorageInstanceProvider failed to start", e);
			throw e;
		}
		log.logInfoUsingMessage("CachedDatabaseStorageInstanceProvider started");
	}

	private void startStorage() {
		databaseLookupValue = SettingsProvider.getSetting(LOOKUP_NAME);
		doNotCacheValue = readDoNotCacheSetting();
		createDependenciesAndStartStorage();
	}

	private void createDependenciesAndStartStorage() {
		RecordStorage dbStorage = startDbStorage();
		setStaticInstance(dbStorage);
	}

	static void setStaticInstance(RecordStorage recordStorage) {
		DatabaseStorageInstance.setInstance(recordStorage);
	}

	private RecordStorage startDbStorage() {
		SqlDatabaseFactory sqlDatabaseFactory = SqlDatabaseFactoryImp
				.usingLookupNameFromContext(databaseLookupValue);
		JsonParser jsonParser = new OrgJsonParser();
		database = createDatabaseRecordStorage(sqlDatabaseFactory, jsonParser);
		if (doNotCache()) {
			log.logInfoUsingMessage("starting direct DatabaseRecordStorage");
			return database;
		}
		log.logInfoUsingMessage("starting in memory cached DatabaseRecordStorage");
		memory = createRecordStorageInMemory();
		return populateFromDatabase(sqlDatabaseFactory, jsonParser, database, memory);
	}

	protected RecordStorage createDatabaseRecordStorage(SqlDatabaseFactory sqlDatabaseFactory,
			JsonParser jsonParser) {
		return new DatabaseRecordStorage(sqlDatabaseFactory, jsonParser);
	}

	protected RecordStorage createRecordStorageInMemory() {
		return new RecordStorageInMemory();
	}

	private boolean readDoNotCacheSetting() {
		try {
			String setting = SettingsProvider.getSetting("doNotCache");
			return "true".equals(setting);
		} catch (InitializationException e) {
			return false;
		}
	}

	private boolean doNotCache() {
		return doNotCacheValue;
	}

	private CachedDatabaseRecordStorage populateFromDatabase(SqlDatabaseFactory sqlDatabaseFactory,
			JsonParser jsonParser, RecordStorage database, RecordStorage memory) {
		FromDbStoragePopulator populator = createPopulater(sqlDatabaseFactory, jsonParser);
		populator.populateStorageFromDatabase(memory);
		return CachedDatabaseRecordStorage.usingDatabaseAndMemory(database, memory);
	}

	protected FromDbStoragePopulator createPopulater(SqlDatabaseFactory sqlDatabaseFactory,
			JsonParser jsonParser) {
		return new FromDbStoragePopulatorImp(sqlDatabaseFactory.factorDatabaseFacade(), jsonParser);
	}

	@Override
	public void dataChanged(String type, String id, String action) {
		if (cacheData()) {
			handleDataCache(type, id, action);
		}
	}

	private boolean cacheData() {
		return !doNotCache();
	}

	private void handleDataCache(String type, String id, String action) {
		if ("delete".equals(action)) {
			memory.deleteByTypeAndId(type, id);
		} else {
			setDataInCache(type, id, action);
		}
	}

	private void setDataInCache(String type, String id, String action) {
		DataRecordGroup dataRecordGroup = database.read(type, id);
		String dataDivider = dataRecordGroup.getDataDivider();
		DataGroup dataGroup = DataProvider.createGroupFromRecordGroup(dataRecordGroup);
		Set<StorageTerm> storageTermsForRecord = database.getStorageTermsForRecord(type, id);
		Set<Link> linksFromRecord = database.getLinksFromRecord(type, id);
		if ("create".equals(action)) {
			memory.create(type, id, dataGroup, storageTermsForRecord, linksFromRecord, dataDivider);
		}
		if ("update".equals(action)) {
			memory.update(type, id, dataGroup, storageTermsForRecord, linksFromRecord, dataDivider);
		}
	}
}
