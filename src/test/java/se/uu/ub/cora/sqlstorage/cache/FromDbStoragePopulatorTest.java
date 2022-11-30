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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.data.collected.Link;
import se.uu.ub.cora.data.collected.StorageTerm;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.sqlstorage.spy.data.DatabaseFacadeSpy;
import se.uu.ub.cora.sqlstorage.spy.json.JsonParserSpy;
import se.uu.ub.cora.sqlstorage.spy.json.JsonToDataConverterFactorySpy;
import se.uu.ub.cora.sqlstorage.spy.json.JsonToDataConverterSpy;
import se.uu.ub.cora.sqlstorage.spy.sql.RowSpy;
import se.uu.ub.cora.storage.spies.RecordStorageSpy;

public class FromDbStoragePopulatorTest {
	private FromDbStoragePopulator populator;
	private JsonParserSpy jsonParserSpy;
	private JsonToDataConverterFactorySpy factoryCreatorSpy;
	private DatabaseFacadeSpy dbFacadeSpy;
	private RecordStorageSpy recordStorageInMemory;
	private int callNo = 0;

	@BeforeMethod
	public void beforeMethod() {
		dbFacadeSpy = new DatabaseFacadeSpy();
		jsonParserSpy = new JsonParserSpy();
		factoryCreatorSpy = new JsonToDataConverterFactorySpy();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(factoryCreatorSpy);
		recordStorageInMemory = new RecordStorageSpy();

		callNo = 0;

		populator = new FromDbStoragePopulatorImp(dbFacadeSpy, jsonParserSpy);
	}

	@Test
	public void testPopulateReadsRecordsFromDb() throws Exception {
		populator.populateStorageFromDatabase(recordStorageInMemory);

		dbFacadeSpy.MCR.assertMethodWasCalled("readUsingSqlAndValues");
		String sql = "select * from storageterm";
		List<Object> values = Collections.emptyList();
		dbFacadeSpy.MCR.assertParameters("readUsingSqlAndValues", 0, sql, values);

		dbFacadeSpy.MCR.assertMethodWasCalled("readUsingSqlAndValues");
		String sqlLinks = "select * from link";
		List<Object> sqlValues = Collections.emptyList();
		dbFacadeSpy.MCR.assertParameters("readUsingSqlAndValues", 1, sqlLinks, sqlValues);

		String sql2 = "select * from record";
		List<Object> values2 = Collections.emptyList();
		dbFacadeSpy.MCR.assertParameters("readUsingSqlAndValues", 2, sql2, values2);
	}

	@Test
	public void testPopulate_twoRecords() throws Exception {
		RowSpy row1 = createRecords();
		Set<Link> links1 = createLinks();
		Set<StorageTerm> storageTermsRow1 = createStorageTerms();

		populator.populateStorageFromDatabase(recordStorageInMemory);

		String typeRow1 = assertAndGetReturnForColumn(row1, "type");
		String idRow1 = assertAndGetReturnForColumn(row1, "id");
		String dataRow1 = assertAndGetReturnForColumn(row1, "data");
		String dataDividerRow1 = assertAndGetReturnForColumn(row1, "datadivider");

		jsonParserSpy.MCR.assertParameters("parseString", 0, dataRow1);
		var jsonRow1 = jsonParserSpy.MCR.getReturnValue("parseString", 0);

		factoryCreatorSpy.MCR.assertParameters("createForJsonObject", 0, jsonRow1);
		JsonToDataConverterSpy converterRow1 = (JsonToDataConverterSpy) factoryCreatorSpy.MCR
				.getReturnValue("createForJsonObject", 0);
		var dataGroupRow1 = converterRow1.MCR.getReturnValue("toInstance", 0);

		recordStorageInMemory.MCR.assertParameter("create", 0, "type", typeRow1);
		recordStorageInMemory.MCR.assertParameter("create", 0, "id", idRow1);
		recordStorageInMemory.MCR.assertParameter("create", 0, "dataRecord", dataGroupRow1);
		recordStorageInMemory.MCR.assertParameterAsEqual("create", 0, "storageTerms",
				storageTermsRow1);
		recordStorageInMemory.MCR.assertParameterAsEqual("create", 0, "links", links1);
		recordStorageInMemory.MCR.assertParameter("create", 0, "dataDivider", dataDividerRow1);

		recordStorageInMemory.MCR.assertNumberOfCallsToMethod("create", 2);
	}

	private RowSpy createRecords() {
		RowSpy record1 = new RowSpy();
		record1.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "type1", "type");
		record1.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "id1", "id");
		RowSpy row2 = new RowSpy();
		dbFacadeSpy.MRV.setSpecificReturnValuesSupplier("readUsingSqlAndValues",
				() -> List.of(record1, row2), "select * from record", Collections.emptyList());
		return record1;
	}

	private Set<Link> createLinks() {
		RowSpy link1 = new RowSpy();
		RowSpy link2 = new RowSpy();
		link2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "type1", "fromtype");
		link2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "id1", "fromid");
		link2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "toType2", "totype");
		link2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "toId2", "toid");
		Set<Link> links1 = new LinkedHashSet<>();
		links1.add(new Link("toType2", "toId2"));

		dbFacadeSpy.MRV.setSpecificReturnValuesSupplier("readUsingSqlAndValues",
				() -> List.of(link1, link2), "select * from link", Collections.emptyList());
		return links1;
	}

	private Set<StorageTerm> createStorageTerms() {
		RowSpy storageTerm1 = new RowSpy();
		RowSpy storageTerm2 = new RowSpy();
		storageTerm2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "type1",
				"recordtype");
		storageTerm2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "id1",
				"recordid");
		storageTerm2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "storagetermid2",
				"storagetermid");
		storageTerm2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "value2",
				"value");
		storageTerm2.MRV.setSpecificReturnValuesSupplier("getValueByColumn", () -> "storagekey2",
				"storagekey");
		Set<StorageTerm> storageTermsRow1 = new LinkedHashSet<>();
		storageTermsRow1.add(new StorageTerm("storagetermid2", "storagekey2", "value2"));

		dbFacadeSpy.MRV.setSpecificReturnValuesSupplier("readUsingSqlAndValues",
				() -> List.of(storageTerm1, storageTerm2), "select * from storageterm",
				Collections.emptyList());
		return storageTermsRow1;
	}

	private String assertAndGetReturnForColumn(RowSpy row1, String column) {
		row1.MCR.assertParameters("getValueByColumn", callNo, column);
		String typeRow1 = (String) row1.MCR.getReturnValue("getValueByColumn", callNo);
		callNo++;
		return typeRow1;
	}

	@Test
	public void testPopulate_noStorageTerms() throws Exception {
		createRecords();
		createLinks();

		populator.populateStorageFromDatabase(recordStorageInMemory);

		recordStorageInMemory.MCR.assertParameter("create", 1, "storageTerms",
				Collections.emptySet());
	}

	@Test
	public void testPopulate_noLinks() throws Exception {
		createRecords();
		createStorageTerms();

		populator.populateStorageFromDatabase(recordStorageInMemory);

		recordStorageInMemory.MCR.assertParameter("create", 1, "links", Collections.emptySet());
	}

}
