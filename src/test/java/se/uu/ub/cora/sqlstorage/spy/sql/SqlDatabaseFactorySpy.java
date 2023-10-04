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

import se.uu.ub.cora.sqldatabase.DatabaseFacade;
import se.uu.ub.cora.sqldatabase.SqlDatabaseFactory;
import se.uu.ub.cora.sqldatabase.table.TableFacade;
import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.sqlstorage.spy.data.DatabaseFacadeSpy;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class SqlDatabaseFactorySpy implements SqlDatabaseFactory {
	public boolean throwExceptionFromTableFacadeOnRead = false;
	public boolean throwNotFoundExceptionFromTableFacadeOnRead = false;
	public boolean throwDataExceptionFromTableFacadeOnRead = false;
	public boolean throwExceptionFromTableFacadeOnUpdate = false;
	public boolean throwExceptionFromTableFacadeOnDelete = false;
	public long totalNumberOfRecordsForType = 0;
	public boolean throwDuplicateExceptionFromTableFacade = false;
	public boolean throwSqlExceptionFromTableFacade = false;
	public int numberOfAffectedRows = 0;
	public boolean usingTransaction = false;

	public MethodCallRecorder MCR = new MethodCallRecorder();
	public MethodReturnValues MRV = new MethodReturnValues();

	public SqlDatabaseFactorySpy() {
		MCR.useMRV(MRV);
		MRV.setDefaultReturnValuesSupplier("factorDatabaseFacade", DatabaseFacadeSpy::new);
		MRV.setDefaultReturnValuesSupplier("factorTableFacade", DatabaseFacadeSpy::new);
		MRV.setDefaultReturnValuesSupplier("factorTableQuery", TableQuerySpy::new);
	}

	@Override
	public DatabaseFacade factorDatabaseFacade() {
		return (DatabaseFacade) MCR.addCallAndReturnFromMRV();
	}

	@Override
	public TableFacade factorTableFacade() {
		MCR.addCall();
		TableFacadeSpy tableFacadeSpy = new TableFacadeSpy();
		tableFacadeSpy.throwExceptionOnRead = throwExceptionFromTableFacadeOnRead;
		tableFacadeSpy.throwNotFoundExceptionOnRead = throwNotFoundExceptionFromTableFacadeOnRead;
		tableFacadeSpy.throwDataExceptionOnRead = throwDataExceptionFromTableFacadeOnRead;
		tableFacadeSpy.throwExceptionOnUpdate = throwExceptionFromTableFacadeOnUpdate;
		tableFacadeSpy.throwExceptionOnDelete = throwExceptionFromTableFacadeOnDelete;
		tableFacadeSpy.throwDuplicateException = throwDuplicateExceptionFromTableFacade;
		tableFacadeSpy.throwSqlException = throwSqlExceptionFromTableFacade;
		tableFacadeSpy.totalNumberOfRecordsForType = totalNumberOfRecordsForType;
		tableFacadeSpy.numberOfAffectedRows = numberOfAffectedRows;
		tableFacadeSpy.usingTransaction = usingTransaction;
		MCR.addReturned(tableFacadeSpy);
		return tableFacadeSpy;
	}

	@Override
	public TableQuery factorTableQuery(String tableName) {
		return (TableQuery) MCR.addCallAndReturnFromMRV("tableName", tableName);
	}

}
