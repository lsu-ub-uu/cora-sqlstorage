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

import java.util.List;

import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.sqldatabase.SqlDatabaseException;
import se.uu.ub.cora.sqldatabase.table.TableFacade;
import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class TableFacadeSpy implements TableFacade {
	MethodCallRecorder MCR = new MethodCallRecorder();
	public boolean throwExceptionOnRead = false;

	@Override
	public void close() {
		MCR.addCall();
	}

	@Override
	public void insertRowUsingQuery(TableQuery tableQuery) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Row> readRowsForQuery(TableQuery tableQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Row readOneRowForQuery(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);
		if (throwExceptionOnRead) {
			throw SqlDatabaseException.withMessage("Error from spy");
		}
		RowSpy result = new RowSpy();

		MCR.addReturned(result);
		return result;
	}

	@Override
	public long readNumberOfRows(TableQuery tableQuery) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void updateRowsUsingQuery(TableQuery tableQuery) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRowsForQuery(TableQuery tableQuery) {
		// TODO Auto-generated method stub

	}

	@Override
	public long nextValueFromSequence(String sequenceName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void startTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public void endTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub

	}
}
