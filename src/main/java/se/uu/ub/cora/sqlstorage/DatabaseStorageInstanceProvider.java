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
package se.uu.ub.cora.sqlstorage;

import se.uu.ub.cora.initialize.SettingsProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.logger.Logger;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.internal.DatabaseStorageInstance;
import se.uu.ub.cora.storage.RecordStorageInstanceProvider;

public class DatabaseStorageInstanceProvider implements RecordStorageInstanceProvider {

	private Logger log = LoggerProvider.getLoggerForClass(DatabaseStorageInstanceProvider.class);
	private static final String LOOKUP_NAME = "coraDatabaseLookupName";
	private String databaseLookupValue;

	@Override
	public int getOrderToSelectImplementionsBy() {
		return -10;
	}

	@Override
	public DatabaseRecordStorage getRecordStorage() {
		possiblyStartStorage();
		return DatabaseStorageInstance.getInstance();
	}

	static void setStaticInstance(DatabaseRecordStorage recordStorage) {
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
				"DatabaseStorageInstanceProvider starting DatabaseRecordStorage...");
		startStorage();
		log.logInfoUsingMessage("DatabaseStorageInstanceProvider started DatabaseRecordStorage");
	}

	private void startStorage() {
		databaseLookupValue = SettingsProvider.getSetting(LOOKUP_NAME);
		createDependenciesAndStartStorage();
	}

	private void createDependenciesAndStartStorage() {
		SqlDatabaseFactory sqlDatabaseFactory = SqlDatabaseFactoryImp
				.usingLookupNameFromContext(databaseLookupValue);
		JsonParser jsonParser = new OrgJsonParser();
		setStaticInstance(new DatabaseRecordStorage(sqlDatabaseFactory, jsonParser));
	}
}
