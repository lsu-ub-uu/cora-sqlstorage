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
import se.uu.ub.cora.sqlstorage.internal.DatabaseRecordStorageInstance;
import se.uu.ub.cora.storage.RecordStorageProvider;
import se.uu.ub.cora.storage.StorageException;

public class DatabaseStorageProvider implements RecordStorageProvider {

	private Logger log = LoggerProvider.getLoggerForClass(DatabaseStorageProvider.class);
	private static final String LOOKUP_NAME = "coraDatabaseLookupName";
	private Map<String, String> initInfo;

	@Override
	public int getOrderToSelectImplementionsBy() {
		return 0;
	}

	@Override
	public void startUsingInitInfo(Map<String, String> initInfo) {
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
		return DatabaseRecordStorageInstance.getInstance() == null;
	}

	private void logAndStartStorage() {
		log.logInfoUsingMessage("DatabaseStorageProvider starting DatabaseRecordStorage...");
		startStorage();
		log.logInfoUsingMessage("DatabaseStorageProvider started DatabaseRecordStorage");
	}

	private void startStorage() {
		tryToGetInitParameterLogIfFoundThrowErrorIfNot(LOOKUP_NAME);
		createDependenciesAndStartStorage();
	}

	private String tryToGetInitParameterLogIfFoundThrowErrorIfNot(String parameterName) {
		String basePath = tryToGetInitParameter(parameterName);
		log.logInfoUsingMessage("Found " + basePath + " as " + parameterName);
		return basePath;
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
				.usingLookupNameFromContext(LOOKUP_NAME);
		JsonParser jsonParser = new OrgJsonParser();
		setStaticInstance(new DatabaseRecordStorage(sqlDatabaseFactory, jsonParser));
	}

	private void logDatabaseStorageAlreadyStarted() {
		log.logInfoUsingMessage("DatabaseRecordStorage already started, using that instance.");
	}

	@Override
	public synchronized DatabaseRecordStorage getRecordStorage() {
		throwErrorIfStorageNotStarted();
		return DatabaseRecordStorageInstance.getInstance();
	}

	private void throwErrorIfStorageNotStarted() {
		if (storageNotStarted()) {
			throw StorageException.withMessage(
					"DatabaseStorageProvider not started, please call startUsingInitInfo first.");
		}
	}

	static void setStaticInstance(DatabaseRecordStorage recordStorage) {
		DatabaseRecordStorageInstance.setInstance(recordStorage);
	}

}
