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
package se.uu.ub.cora.sqlstorage.cache;

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

public class FromDbStoragePopulatorImp implements FromDbStoragePopulator {

	private static final List<Object> EMPTY_LIST = Collections.emptyList();
	private DatabaseFacade dbFacade;
	private JsonParser jsonParser;
	private RecordStorage recordStorageInMemory;
	private InternalHolder internalHolder;

	public FromDbStoragePopulatorImp(DatabaseFacade dbFacade, JsonParser jsonParser) {
		this.dbFacade = dbFacade;
		this.jsonParser = jsonParser;
		internalHolder = new InternalHolder();
	}

	@Override
	public void populateStorageFromDatabase(RecordStorage recordStorageInMemory) {
		this.recordStorageInMemory = recordStorageInMemory;
		readAllStorageTermsAndSaveIntoInternalHolder();
		readAllLinksAndSaveIntoInternalHolder();
		readAllRecordsAndPopulateStorageInMemory();
	}

	private void readAllStorageTermsAndSaveIntoInternalHolder() {
		List<Row> storageTermRows = dbFacade.readUsingSqlAndValues("select * from storageterm",
				EMPTY_LIST);
		for (Row row : storageTermRows) {
			addStorageTermToInternalHolder(row);
		}
	}

	private void addStorageTermToInternalHolder(Row row) {
		String recordtype = getColumnFromRow(row, "recordtype");
		String recordid = getColumnFromRow(row, "recordid");
		String storageTermId = getColumnFromRow(row, "storagetermid");
		String value = getColumnFromRow(row, "value");
		String storageKey = getColumnFromRow(row, "storagekey");
		internalHolder.addStorageTerm(recordtype, recordid, storageTermId, storageKey, value);
	}

	private String getColumnFromRow(Row row, String columnName) {
		return (String) row.getValueByColumn(columnName);
	}

	private void readAllLinksAndSaveIntoInternalHolder() {
		List<Row> linksRows = dbFacade.readUsingSqlAndValues("select * from link", EMPTY_LIST);
		for (Row row : linksRows) {
			addLinkToInternalHolder(row);
		}
	}

	private void addLinkToInternalHolder(Row row) {
		String fromtype = getColumnFromRow(row, "fromtype");
		String fromid = getColumnFromRow(row, "fromid");
		String totype = getColumnFromRow(row, "totype");
		String toid = getColumnFromRow(row, "toid");
		internalHolder.addLink(fromtype, fromid, totype, toid);
	}

	private void readAllRecordsAndPopulateStorageInMemory() {
		List<Row> dataRows = dbFacade.readUsingSqlAndValues("select * from record", EMPTY_LIST);
		for (Row row : dataRows) {
			createRecordInMemoryStorage(row);
		}
	}

	private void createRecordInMemoryStorage(Row row) {
		String type = getColumnFromRow(row, "type");
		String id = getColumnFromRow(row, "id");
		String data = getColumnFromRow(row, "data");
		String dataDivider = getColumnFromRow(row, "datadivider");

		DataGroup dataRecordGroup = convertJsonToDataGroup(data);

		populateStorageInMemory(type, id, dataDivider, dataRecordGroup);
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
		return (DataGroup) jsonToDataConverter.toInstance();
	}

	private class InternalHolder {
		Map<String, Set<StorageTerm>> storageTerms = new HashMap<>();
		Map<String, Set<Link>> links = new HashMap<>();

		public void addStorageTerm(String type, String id, String storageTermId, String storageKey,
				String value) {
			String combined = combine(type, id);
			StorageTerm storageTerm = new StorageTerm(storageTermId, storageKey, value);
			storageTerms.computeIfAbsent(combined, k -> new LinkedHashSet<>()).add(storageTerm);
		}

		public void addLink(String fromType, String fromId, String toType, String toId) {
			String combined = combine(fromType, fromId);
			Link link = new Link(toType, toId);
			links.computeIfAbsent(combined, k -> new LinkedHashSet<Link>()).add(link);
		}

		private String combine(String type, String id) {
			return type + "-" + id;
		}

		public Set<StorageTerm> getStorageTemSet(String type, String id) {
			return storageTerms.getOrDefault(combine(type, id), Collections.emptySet());
		}

		public Set<Link> getLinkSet(String type, String id) {
			return links.getOrDefault(combine(type, id), Collections.emptySet());
		}

	}

	public DatabaseFacade onlyForTestGetDatabaseFacade() {
		return dbFacade;
	}

	public JsonParser onlyForTestGetJsonParser() {
		return jsonParser;
	}
}
