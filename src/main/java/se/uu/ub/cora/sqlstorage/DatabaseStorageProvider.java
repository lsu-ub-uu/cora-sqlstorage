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

import java.util.Map;

import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.org.OrgJsonParser;
import se.uu.ub.cora.logger.Logger;
import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactoryImp;
import se.uu.ub.cora.sqlstorage.internal.DatabaseStorageInstance;
import se.uu.ub.cora.storage.RecordStorageInstanceProvider;
import se.uu.ub.cora.storage.StorageException;

public class DatabaseStorageProvider implements RecordStorageInstanceProvider {

	private Logger log = LoggerProvider.getLoggerForClass(DatabaseStorageProvider.class);
	private static final String LOOKUP_NAME = "coraDatabaseLookupName";
	private Map<String, String> initInfo;
	private String databaseLookupValue;

	@Override
	public int getOrderToSelectImplementionsBy() {
		return 10;
	}

	@Override
	public DatabaseRecordStorage getRecordStorage() {
		// throwErrorIfStorageNotStarted();
		return DatabaseStorageInstance.getInstance();
	}

	private void throwErrorIfStorageNotStarted() {
		if (storageNotStarted()) {
			throw StorageException.withMessage(
					"DatabaseStorageProvider not started, please call startUsingInitInfo first.");
		}
	}

	static void setStaticInstance(DatabaseRecordStorage recordStorage) {
		DatabaseStorageInstance.setInstance(recordStorage);
	}

	// @Override
	public synchronized void startUsingInitInfo(Map<String, String> initInfo) {
		this.initInfo = initInfo;
		possiblyStartStorage();
	}

	private void possiblyStartStorage() {
		if (storageNotStarted()) {
			logAndStartStorage();
		} else {
			logDatabaseStorageAlreadyStarted();
		}
	}

	private boolean storageNotStarted() {
		return DatabaseStorageInstance.getInstance() == null;
	}

	private void logAndStartStorage() {
		log.logInfoUsingMessage("DatabaseStorageProvider starting DatabaseRecordStorage...");
		startStorage();
		log.logInfoUsingMessage("DatabaseStorageProvider started DatabaseRecordStorage");
	}

	private void startStorage() {
		databaseLookupValue = tryToGetInitParameterLogIfFoundThrowErrorIfNot(LOOKUP_NAME);
		createDependenciesAndStartStorage();
	}

	private String tryToGetInitParameterLogIfFoundThrowErrorIfNot(String parameterName) {
		String parameterValue = tryToGetInitParameter(parameterName);
		log.logInfoUsingMessage("Found " + parameterValue + " as " + parameterName);
		return parameterValue;
	}

	private String tryToGetInitParameter(String parameterName) {
		throwErrorIfKeyIsMissingFromInitInfo(parameterName);
		return initInfo.get(parameterName);
	}

	private void throwErrorIfKeyIsMissingFromInitInfo(String key) {
		if (!initInfo.containsKey(key)) {
			String errorMessage = "InitInfo must contain " + key;
			log.logFatalUsingMessage(errorMessage);
			throw StorageException.withMessage(errorMessage);
		}
	}

	private void createDependenciesAndStartStorage() {
		SqlDatabaseFactory sqlDatabaseFactory = SqlDatabaseFactoryImp
				.usingLookupNameFromContext(databaseLookupValue);
		JsonParser jsonParser = new OrgJsonParser();
		setStaticInstance(new DatabaseRecordStorage(sqlDatabaseFactory, jsonParser));
	}

	private void logDatabaseStorageAlreadyStarted() {
		log.logInfoUsingMessage("DatabaseRecordStorage already started, using that instance.");
	}

}
