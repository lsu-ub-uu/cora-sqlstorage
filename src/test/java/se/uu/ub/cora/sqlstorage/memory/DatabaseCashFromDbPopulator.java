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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.converter.JsonToDataConverter;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.sqldatabase.DatabaseFacade;
import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.storage.RecordStorage;

public class DatabaseCashFromDbPopulator {

	private DatabaseFacade dbFacade;
	private JsonParser jsonParser;
	private RecordStorage recordStorageInMemory;
	private InternalHolder internalHolder;

	public DatabaseCashFromDbPopulator(RecordStorage recordStorageInMemory, DatabaseFacade dbFacade,
			JsonParser jsonParser) {
		this.recordStorageInMemory = recordStorageInMemory;
		this.dbFacade = dbFacade;
		this.jsonParser = jsonParser;
		internalHolder = new InternalHolder();
	}

	public RecordStorage createInMemoryFromDatabase() {

		readAllStorageTermsAndSaveIntoInternalHolder();
		readAllLinksAndSaveIntoInternalHolder();
		readAllRecordsAndPopulateStorageInMemory();

		return recordStorageInMemory;
	}

	private void readAllStorageTermsAndSaveIntoInternalHolder() {
		List<Row> storageTermRows = dbFacade.readUsingSqlAndValues("select * from storageterm",
				Collections.emptyList());

		for (Row row : storageTermRows) {
			String recordtype = getColumnFromRow(row, "recordtype");
			String recordid = (String) row.getValueByColumn("recordid");
			String storageTermId = (String) row.getValueByColumn("storagetermid");
			String value = (String) row.getValueByColumn("value");
			String storageKey = (String) row.getValueByColumn("storagekey");
			internalHolder.addStorageTerm(recordtype, recordid, storageTermId, storageKey, value);
		}
	}

	private void readAllLinksAndSaveIntoInternalHolder() {
		List<Row> linksRows = dbFacade.readUsingSqlAndValues("select * from links",
				Collections.emptyList());

		for (Row row : linksRows) {
			String fromtype = getColumnFromRow(row, "fromtype");
			String fromid = (String) row.getValueByColumn("fromid");
			String totype = (String) row.getValueByColumn("totype");
			String toid = (String) row.getValueByColumn("toid");
			internalHolder.addLink(fromtype, fromid, totype, toid);
		}
	}

	private void readAllRecordsAndPopulateStorageInMemory() {
		List<Row> dataRows = dbFacade.readUsingSqlAndValues("select * from record",
				Collections.emptyList());

		for (Row row : dataRows) {
			String type = (String) row.getValueByColumn("type");
			String id = (String) row.getValueByColumn("id");
			String data = (String) row.getValueByColumn("data");
			String dataDivider = (String) row.getValueByColumn("datadivider");

			DataGroup dataRecordGroup = convertJsonToDataGroup(data);

			populateStorageInMemory(type, id, dataDivider, dataRecordGroup);
		}
	}

	private void populateStorageInMemory(String type, String id, String dataDivider,
			DataGroup dataRecordGroup) {
		recordStorageInMemory.create(type, id, dataRecordGroup,
				internalHolder.getStorageTemSet(type, id), internalHolder.getLinkSet(type, id),
				dataDivider);
	}

	private DataGroup convertJsonToDataGroup(String data) {
		JsonValue jsonValue = jsonParser.parseString(data);
		JsonToDataConverter jsonToDataConverter = JsonToDataConverterProvider
				.getConverterUsingJsonObject(jsonValue);
		DataGroup dataRecordGroup = (DataGroup) jsonToDataConverter.toInstance();
		return dataRecordGroup;
	}

	private String getColumnFromRow(Row row, String columnName) {
		return (String) row.getValueByColumn(columnName);
	}

	private class InternalHolder {
		Map<String, Set<StorageTerm>> storageTerms = new HashMap<>();
		Map<String, Set<Link>> links = new HashMap<>();

		public void addStorageTerm(String type, String id, String storageTermId, String storageKey,
				String value) {
			String combined = combine(type, id);

			if (storageTerms.containsKey(combined)) {
				storageTerms.get(combined).add(new StorageTerm(storageTermId, storageKey, value));
			} else {
				Set<StorageTerm> set = new LinkedHashSet<>();
				set.add(new StorageTerm(storageTermId, storageKey, value));

				storageTerms.put(combined, set);
			}
		}

		public void addLink(String fromType, String fromId, String toType, String toId) {
			String combined = combine(fromType, fromId);

			if (links.containsKey(combined)) {
				links.get(combined).add(new Link(toType, toId));
			} else {
				Set<Link> set = new LinkedHashSet<>();
				set.add(new Link(toType, toId));
				links.put(combined, set);
			}
		}

		private String combine(String type, String id) {
			return type + "-" + id;
		}

		public Set<StorageTerm> getStorageTemSet(String type, String id) {
			return storageTerms.get(combine(type, id));
		}

		public Set<Link> getLinkSet(String type, String id) {
			return links.get(combine(type, id));
		}

	}
}
