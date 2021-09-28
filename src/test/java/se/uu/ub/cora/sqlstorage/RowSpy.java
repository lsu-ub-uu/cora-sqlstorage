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

import java.util.Set;

import se.uu.ub.cora.sqldatabase.Row;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class RowSpy implements Row {
	public MethodCallRecorder MCR = new MethodCallRecorder();

	@Override
	public Object getValueByColumn(String columnName) {
		MCR.addCall("columnName", columnName);
		String out = "some value from getValueByColumn in rowSpy";
		MCR.addReturned(out);
		return out;
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

}
