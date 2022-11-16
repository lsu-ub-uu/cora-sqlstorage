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

import java.util.Set;

import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class RowSpy implements Row {
	public MethodCallRecorder MCR = new MethodCallRecorder();
	public MethodReturnValues MRV = new MethodReturnValues();

	public RowSpy() {
		MCR.useMRV(MRV);

		MRV.setDefaultReturnValuesSupplier("getValueByColumn",
				() -> "some value from getValueByColumn in rowSpy");
	}

	@Override
	public Object getValueByColumn(String columnName) {
		return MCR.addCallAndReturnFromMRV("columnName", columnName);
	}

	@Override
	public Set<String> columnSet() {
		MCR.addCall();
		return null;
	}

	@Override
	public boolean hasColumn(String columnName) {
		MCR.addCall("columnName", columnName);
		return false;
	}

	@Override
	public boolean hasColumnWithNonEmptyValue(String columnName) {
		MCR.addCall("columnName", columnName);
		return false;
	}

}
