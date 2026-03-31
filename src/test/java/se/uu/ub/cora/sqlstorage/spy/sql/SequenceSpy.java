/*
 * Copyright 2026 Uppsala University Library
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

import se.uu.ub.cora.sqldatabase.sequence.Sequence;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class SequenceSpy implements Sequence {

	public MethodCallRecorder MCR = new MethodCallRecorder();
	public MethodReturnValues MRV = new MethodReturnValues();

	public SequenceSpy() {
		MCR.useMRV(MRV);
		MRV.setDefaultReturnValuesSupplier("getCurrentValueForSequence", () -> 0L);
		MRV.setDefaultReturnValuesSupplier("getNextValueForSequence", () -> 1L);
	}

	@Override
	public void createSequence(String sequenceName, long startValue) {
		MCR.addCall("sequenceName", sequenceName, "startValue", startValue);
	}

	@Override
	public long getCurrentValueForSequence(String sequenceName) {
		return (long) MCR.addCallAndReturnFromMRV("sequenceName", sequenceName);
	}

	@Override
	public long getNextValueForSequence(String sequenceName) {
		return (long) MCR.addCallAndReturnFromMRV("sequenceName", sequenceName);
	}

	@Override
	public void updateSequenceValue(String sequenceName, long value) {
		MCR.addCall("sequenceName", sequenceName, "value", value);
	}

	@Override
	public void deleteSequence(String sequenceName) {
		MCR.addCall("sequenceName", sequenceName);
	}

	@Override
	public void close() {
		MCR.addCall();
	}
}
