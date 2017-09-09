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

import java.util.Collection;

import se.uu.ub.cora.bookkeeper.data.DataGroup;
import se.uu.ub.cora.spider.record.storage.RecordStorage;

public class RecordStorageInDatabase implements RecordStorage {

	public DataGroup read(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	public void create(String type, String id, DataGroup record, DataGroup linkList,
			String dataDivider) {
		// TODO Auto-generated method stub

	}

	public void deleteByTypeAndId(String type, String id) {
		// TODO Auto-generated method stub

	}

	public boolean linksExistForRecord(String type, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	public void update(String type, String id, DataGroup record, DataGroup linkList,
			String dataDivider) {
		// TODO Auto-generated method stub

	}

	public Collection<DataGroup> readList(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	public Collection<DataGroup> readAbstractList(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	public DataGroup readLinkList(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	public Collection<DataGroup> generateLinkCollectionPointingToRecord(String type, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean recordsExistForRecordType(String type) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean recordExistsForAbstractOrImplementingRecordTypeAndRecordId(String type,
			String id) {
		// TODO Auto-generated method stub
		return false;
	}

}
