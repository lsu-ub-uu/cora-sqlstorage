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
package se.uu.ub.cora.sqlstorage.spy.sql;

import java.util.ArrayList;
import java.util.List;

import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.sqldatabase.SqlConflictException;
import se.uu.ub.cora.sqldatabase.SqlDatabaseException;
import se.uu.ub.cora.sqldatabase.table.TableFacade;
import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class TableFacadeSpy implements TableFacade {
	public MethodCallRecorder MCR = new MethodCallRecorder();
	public boolean throwExceptionOnRead = false;
	public boolean throwExceptionOnUpdate = false;
	public boolean throwExceptionOnDelete = false;
	public long totalNumberOfRecordsForType = 0;
	public boolean throwDuplicateException = false;
	public boolean throwSqlException = false;
	public int numberOfAffectedRows = 0;
	public boolean usingTransaction = false;

	@Override
	public void close() {
		MCR.addCall();
	}

	@Override
	public void insertRowUsingQuery(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);
		if (throwDuplicateException) {
			throw SqlConflictException.withMessage("Error from insertRowUsingQuery in tablespy");
		}
		if (throwSqlException) {
			throw SqlDatabaseException.withMessage("Error from spy");
		}
		if (usingTransaction && startTransactionNotCalled()) {
			throw new RuntimeException(
					"Error from SPY. StartTransaction should be called before insertRowUsingQuery. Controll the order of start end transaction.");
		}
		if (usingTransaction && MCR.methodWasCalled("endTransaction")) {
			throw new RuntimeException(
					"Error from SPY. endTransaction has been called before insertRowUsingQuery. Controll the order of start end transaction.");
		}

	}

	@Override
	public List<Row> readRowsForQuery(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);
		if (throwExceptionOnRead) {
			throw SqlDatabaseException.withMessage("Error from readRowsForQuery in tablespy");
		}
		List<Row> listResult = new ArrayList<>();
		if (totalNumberOfRecordsForType > 0) {
			RowSpy result = new RowSpy();
			RowSpy result2 = new RowSpy();
			RowSpy result3 = new RowSpy();
			listResult.add(result);
			listResult.add(result2);
			listResult.add(result3);
		}

		MCR.addReturned(listResult);
		return listResult;
	}

	@Override
	public Row readOneRowForQuery(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);
		if (throwExceptionOnRead) {
			throw SqlDatabaseException.withMessage("Error from readOneRowForQuery in tablespy");
		}
		RowSpy result = new RowSpy();

		MCR.addReturned(result);
		return result;
	}

	@Override
	public long readNumberOfRows(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);
		if (throwExceptionOnRead) {
			throw SqlDatabaseException.withMessage("Error from readNumberOfRows in tablespy");
		}
		MCR.addReturned(totalNumberOfRecordsForType);
		return totalNumberOfRecordsForType;
	}

	@Override
	public int updateRowsUsingQuery(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);

		if (throwExceptionOnUpdate) {
			throw SqlDatabaseException.withMessage("Error from updateRowsUsingQuery in tablespy");
		}
		if (throwDuplicateException) {
			throw SqlConflictException.withMessage("Error from updateRowsUsingQuery in tablespy");
		}
		if (usingTransaction && MCR.methodWasCalled("endTransaction")) {
			throw new RuntimeException(
					"Error from SPY. endTransaction has been called before updateRowsUsingQuery. Controll the order of start end transaction.");
		}

		MCR.addReturned(numberOfAffectedRows);
		return numberOfAffectedRows;
	}

	@Override
	public int deleteRowsForQuery(TableQuery tableQuery) {
		MCR.addCall("tableQuery", tableQuery);
		if (throwExceptionOnDelete) {
			throw SqlDatabaseException.withMessage("Error from deleteRowsUsingQuery in tablespy");
		}
		if (usingTransaction && startTransactionNotCalled()) {
			throw new RuntimeException(
					"Error from SPY. StartTransaction should be called before insertRowUsingQuery. Controll the order of start end transaction.");
		}
		if (usingTransaction && MCR.methodWasCalled("endTransaction")) {
			throw new RuntimeException(
					"Error from SPY. endTransaction has been called before deleteRowsForQuery. Controll the order of start end transaction.");
		}
		MCR.addReturned(numberOfAffectedRows);
		return numberOfAffectedRows;
	}

	@Override
	public long nextValueFromSequence(String sequenceName) {
		MCR.addCall("sequenceName", sequenceName);
		long result = 0L;
		MCR.addReturned(result);
		return result;
	}

	@Override
	public void startTransaction() {
		MCR.addCall();
	}

	@Override
	public void endTransaction() {
		MCR.addCall();
		if (startTransactionNotCalled()) {
			throw new RuntimeException(
					"Error from SPY. StartTransaction should be called before endTransaction. Controll the order of start end transaction.");
		}
	}

	private boolean startTransactionNotCalled() {
		return !MCR.methodWasCalled("startTransaction");
	}

	@Override
	public void rollback() {
		MCR.addCall();
	}
}
