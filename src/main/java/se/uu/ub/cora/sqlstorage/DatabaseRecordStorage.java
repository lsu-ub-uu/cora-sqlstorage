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

import java.util.Collection;
import java.util.List;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.sqldatabase.SqlDatabaseException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.table.TableFacade;
import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.storage.RecordNotFoundException;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageReadResult;

/**
 * DatabaseRecordStorage provides an implementation of {@link RecordStorage} using a standardized
 * database schema to store records and metadata in the standard Cora JSON format. The database has
 * one table for each recordType. The tables consist of two columns id and dataRecord with the
 * record stored in JSON format.
 */
public class DatabaseRecordStorage implements RecordStorage {

	private SqlDatabaseFactory sqlDatabaseFactory;

	public DatabaseRecordStorage(SqlDatabaseFactory sqlDatabaseFactory) {
		this.sqlDatabaseFactory = sqlDatabaseFactory;
	}

	@Override
	public DataGroup read(String type, String id) {
		try (TableFacade tableFacade = sqlDatabaseFactory.factorTableFacade()) {
			TableQuery tableQuery = sqlDatabaseFactory.factorTableQuery(type);
			tableQuery.addCondition("id", id);
			tableFacade.readOneRowForQuery(tableQuery);
		} catch (SqlDatabaseException e) {
			throw new RecordNotFoundException(
					"No record found for recordType: " + type + " with id: " + id);
		}
		// RecordReader recordReader = readerFactory.factorTableFacade();
		// Map<String, Object> conditions = new HashMap<>();
		// conditions.put("id", id);
		// try {
		// // String preparedStatement = "select * from "+type +"where id=?";
		// // DataReader dr =null;
		// // dr.readOneRowOrFailUsingSqlAndValues(preparedStatement, null)
		// recordReader.readOneRowFromDbUsingTableAndConditions(type, conditions);
		// } catch (SqlDatabaseException e) {
		// throw new RecordNotFoundException(
		// "No record found for recordType: " + type + " with id: " + id);
		// }
		return null;
	}

	@Override
	public void create(String type, String id, DataGroup record, DataGroup collectedTerms,
			DataGroup linkList, String dataDivider) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteByTypeAndId(String type, String id) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void update(String type, String id, DataGroup record, DataGroup collectedTerms,
			DataGroup linkList, String dataDivider) {
		// TODO Auto-generated method stub

	}

	@Override
	public StorageReadResult readList(String type, DataGroup filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StorageReadResult readAbstractList(String type, DataGroup filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataGroup readLinkList(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String type,
			String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getTotalNumberOfRecordsForType(String type, DataGroup filter) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getTotalNumberOfRecordsForAbstractType(String abstractType,
			List<String> implementingTypes, DataGroup filter) {
		// TODO Auto-generated method stub
		return 0;
	}

}
