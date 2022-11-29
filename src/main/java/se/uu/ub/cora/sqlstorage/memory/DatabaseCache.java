/*
 * Copyright 2022 Uppsala University Library
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
package se.uu.ub.cora.sqlstorage.memory;

import java.util.List;
import java.util.Set;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageReadResult;

public class DatabaseCache implements RecordStorage {

	private RecordStorage database;
	private RecordStorage memory;

	public static DatabaseCache usingDatabaseAndMemory(RecordStorage database,
			RecordStorage memory) {
		return new DatabaseCache(database, memory);
	}

	private DatabaseCache(RecordStorage database, RecordStorage memory) {
		this.database = database;
		this.memory = memory;
	}

	@Override
	public DataGroup read(List<String> types, String id) {
		return memory.read(types, id);
	}

	@Override
	public void create(String type, String id, DataGroup dataRecord, Set<StorageTerm> storageTerms,
			Set<Link> links, String dataDivider) {
		memory.create(type, id, dataRecord, storageTerms, links, dataDivider);
		database.create(type, id, dataRecord, storageTerms, links, dataDivider);

	}

	@Override
	public void deleteByTypeAndId(String type, String id) {
		memory.deleteByTypeAndId(type, id);
		database.deleteByTypeAndId(type, id);

	}

	@Override
	public void update(String type, String id, DataGroup dataRecord, Set<StorageTerm> storageTerms,
			Set<Link> links, String dataDivider) {
		memory.update(type, id, dataRecord, storageTerms, links, dataDivider);
		database.update(type, id, dataRecord, storageTerms, links, dataDivider);

	}

	@Override
	public StorageReadResult readList(List<String> types, Filter filter) {
		return memory.readList(types, filter);
	}

	@Override
	public boolean recordExists(List<String> types, String id) {
		return memory.recordExists(types, id);
	}

	@Override
	public boolean linksExistForRecord(String type, String id) {
		return memory.linksExistForRecord(type, id);
	}

	@Override
	public Set<Link> getLinksToRecord(String type, String id) {
		return memory.getLinksToRecord(type, id);
	}

	@Override
	public long getTotalNumberOfRecordsForTypes(List<String> types, Filter filter) {
		return memory.getTotalNumberOfRecordsForTypes(types, filter);
	}

}
