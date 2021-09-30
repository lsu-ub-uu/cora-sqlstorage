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

import se.uu.ub.cora.data.Convertible;
import se.uu.ub.cora.data.converter.DataToJsonConverter;
import se.uu.ub.cora.data.converter.DataToJsonConverterFactory;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class DataToJsonConverterFactorySpy implements DataToJsonConverterFactory {

	DataToJsonConverter dataToJsonConverter = new DataToJsonConverterSpy();
	MethodCallRecorder MCR = new MethodCallRecorder();

	@Override
	public DataToJsonConverter factorUsingConvertible(Convertible convertible) {
		MCR.addCall("convertible", convertible);
		MCR.addReturned(dataToJsonConverter);
		return dataToJsonConverter;
	}

	@Override
	public DataToJsonConverter factorUsingBaseUrlAndConvertible(String baseUrl,
			Convertible convertible) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataToJsonConverter factorUsingBaseUrlAndRecordUrlAndConvertible(String baseUrl,
			String recordUrl, Convertible convertible) {
		// TODO Auto-generated method stub
		return null;
	}

}
