/**
  ~ Copyright 2017 Olov McKie
  ~
  ~ This file is part of Cora.
  ~
  ~     Cora is free software: you can redistribute it and/or modify
  ~     it under the terms of the GNU General Public License as published by
  ~     the Free Software Foundation, either version 3 of the License, or
  ~     (at your option) any later version.
  ~
  ~     Cora is distributed in the hope that it will be useful,
  ~     but WITHOUT ANY WARRANTY; without even the implied warranty of
  ~     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  ~     GNU General Public License for more details.
  ~
  ~     You should have received a copy of the GNU General Public License
  ~     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
*/
package se.uu.ub.cora.sqlstorage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.bookkeeper.data.converter.DataGroupToJsonConverter;
import se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter;
import se.uu.ub.cora.postgres.SqlConnectionProvider;
import se.uu.ub.cora.spider.record.storage.RecordStorage;

public class RecordStorageInDatabase implements RecordStorage {

	private SqlConnectionProvider sqlConnectionProvider;

	public static RecordStorageInDatabase usingSqlConnectionProvider(
			SqlConnectionProvider sqlConnectionProvider) {
		return new RecordStorageInDatabase(sqlConnectionProvider);
	}

	private RecordStorageInDatabase(SqlConnectionProvider sqlConnectionProvider) {
		this.sqlConnectionProvider = sqlConnectionProvider;
		// TODO Auto-generated constructor stub
	}

	@Override
	public DataGroup read(String type, String id) {
		// TODO Auto-generated method stub

		// trams test
		try {
			Connection con = sqlConnectionProvider.getConnection();
			PreparedStatement prepareStatement = con.prepareStatement("select * from trying;");
			ResultSet resultSet = prepareStatement.executeQuery();
			while (resultSet.next()) {
				String idRead = resultSet.getString("id");
				String stuff = resultSet.getString("stuff");
				String x = "";
				x += "1";
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void create(String type, String id, DataGroup record, DataGroup linkList,
			String dataDivider) {
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
	public void update(String type, String id, DataGroup record, DataGroup linkList,
			String dataDivider) {
		// TODO Auto-generated method stub
		// trams test2 insert
		try {
			Connection con = sqlConnectionProvider.getConnection();
			PreparedStatement preparedStatement = con.prepareStatement(
					"insert into records (recordType, recordId, dataDivider, data) values (?, ?, ?, to_json(?::json))");
			String jsonString = convertDataGroupToJsonString(record);
			// create the mysql insert preparedstatement
			preparedStatement.setString(1, type);
			preparedStatement.setString(2, id);
			preparedStatement.setString(3, dataDivider);
			preparedStatement.setString(4, jsonString);

			// execute the preparedstatement
			preparedStatement.execute();

			// ResultSet resultSet = prepareStatement.executeQuery();
			// while (resultSet.next()) {
			// String idRead = resultSet.getString("id");
			// String stuff = resultSet.getString("stuff");
			String x = "";
			x += "1";
			// }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = createDataGroupToJsonConvert(dataGroup);
		return dataToJsonConverter.toJson();
	}

	private DataGroupToJsonConverter createDataGroupToJsonConvert(DataGroup dataGroup) {
		se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new OrgJsonBuilderFactoryAdapter();
		return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory, dataGroup);
	}

	@Override
	public Collection<DataGroup> readList(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<DataGroup> readAbstractList(String type) {
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
	public boolean recordsExistForRecordType(String type) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String type,
			String id) {
		// TODO Auto-generated method stub
		return false;
	}

	public SqlConnectionProvider getSqlConnectionProvider() {
		return sqlConnectionProvider;
	}

}
