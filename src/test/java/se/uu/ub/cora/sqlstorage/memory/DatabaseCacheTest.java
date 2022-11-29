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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.spies.DataGroupSpy;
import se.uu.ub.cora.storage.Filter;
import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.storage.StorageReadResult;
import se.uu.ub.cora.storage.spies.RecordStorageSpy;

public class DatabaseCacheTest {

	private RecordStorageSpy database;
	private RecordStorageSpy memory;
	private RecordStorage db;
	private List<String> types;
	private String id = "someId";
	private String dataDivider = "someDataDivider";
	private String type = "someType";
	private Set<StorageTerm> storageTerms = Set.of();
	private Set<Link> links = Set.of();
	private DataGroupSpy dataRecord = new DataGroupSpy();
	private Filter filter = new Filter();

	@BeforeMethod
	public void beforeMethod() {
		database = new RecordStorageSpy();
		memory = new RecordStorageSpy();
		db = DatabaseCache.usingDatabaseAndMemory(database, memory);

		types = List.of("someType");
	}

	@Test
	public void testReadSentToMemory() throws Exception {
		DataGroup result = db.read(types, id);

		memory.MCR.assertParameters("read", 0, types, id);
		memory.MCR.assertReturn("read", 0, result);

		database.MCR.assertMethodNotCalled("read");
	}

	@Test
	public void testCreateSentToMemoryAndDatabase() throws Exception {
		db.create(type, id, dataRecord, storageTerms, links, dataDivider);

		memory.MCR.assertParameters("create", 0, type, id, dataRecord, storageTerms, links,
				dataDivider);

		database.MCR.assertParameters("create", 0, type, id, dataRecord, storageTerms, links,
				dataDivider);
	}

	@Test
	public void testDeleteSentToMemoryAndDatabase() throws Exception {
		db.deleteByTypeAndId(type, id);

		memory.MCR.assertParameters("deleteByTypeAndId", 0, type, id);

		database.MCR.assertParameters("deleteByTypeAndId", 0, type, id);
	}

	@Test
	public void testUpdateSentToMemoryAndDatabase() throws Exception {
		db.update(type, id, dataRecord, storageTerms, links, dataDivider);

		memory.MCR.assertParameters("update", 0, type, id, dataRecord, storageTerms, links,
				dataDivider);

		database.MCR.assertParameters("update", 0, type, id, dataRecord, storageTerms, links,
				dataDivider);
	}

	@Test
	public void testReadListSentToMemory() throws Exception {
		StorageReadResult result = db.readList(types, filter);

		memory.MCR.assertParameters("readList", 0, types, filter);
		memory.MCR.assertReturn("readList", 0, result);

		database.MCR.assertMethodNotCalled("readList");
	}

	@Test
	public void testRecordExistsSentToMemory() throws Exception {
		boolean result = db.recordExists(types, id);

		memory.MCR.assertParameters("recordExists", 0, types, id);
		memory.MCR.assertReturn("recordExists", 0, result);

		database.MCR.assertMethodNotCalled("recordExists");
	}

	@Test
	public void testLinksExistsSentToMemory() throws Exception {
		boolean result = db.linksExistForRecord(type, id);

		memory.MCR.assertParameters("linksExistForRecord", 0, type, id);
		memory.MCR.assertReturn("linksExistForRecord", 0, result);

		database.MCR.assertMethodNotCalled("recordExists");
	}

	@Test
	public void testGetLinksToRecordSentToMemory() throws Exception {
		Set<Link> result = db.getLinksToRecord(type, id);

		memory.MCR.assertParameters("getLinksToRecord", 0, type, id);
		memory.MCR.assertReturn("getLinksToRecord", 0, result);

		database.MCR.assertMethodNotCalled("getLinksToRecord");
	}

	@Test
	public void testGetTotalNumberOfRecordsForTypeSentToMemory() throws Exception {
		long result = db.getTotalNumberOfRecordsForTypes(types, filter);

		memory.MCR.assertParameters("getTotalNumberOfRecordsForTypes", 0, type, filter);
		memory.MCR.assertReturn("getTotalNumberOfRecordsForTypes", 0, result);

		database.MCR.assertMethodNotCalled("getTotalNumberOfRecordsForTypes");
	}
}
