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
package se.uu.ub.cora.sqlstorage.spy.json;

import se.uu.ub.cora.json.parser.JsonArray;
import se.uu.ub.cora.json.parser.JsonObject;
import se.uu.ub.cora.json.parser.JsonParser;
import se.uu.ub.cora.json.parser.JsonValue;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class JsonParserSpy implements JsonParser {

	public MethodCallRecorder MCR = new MethodCallRecorder();

	@Override
	public JsonValue parseString(String jsonString) {
		MCR.addCall("jsonString", jsonString);

		JsonValueSpy jsonValue = new JsonValueSpy();
		MCR.addReturned(jsonValue);
		return jsonValue;
	}

	@Override
	public JsonObject parseStringAsObject(String jsonString) {
		MCR.addCall("jsonString", jsonString);

		JsonObject jsonObject = new JsonObjectSpy();
		MCR.addReturned(jsonObject);
		return jsonObject;
	}

	@Override
	public JsonArray parseStringAsArray(String jsonString) {
		MCR.addCall("jsonString", jsonString);

		JsonArray jsonArray = new JsonArraySpy();
		MCR.addReturned(jsonArray);
		return jsonArray;
	}

}
