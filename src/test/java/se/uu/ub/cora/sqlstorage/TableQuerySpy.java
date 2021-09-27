package se.uu.ub.cora.sqlstorage;

import java.util.List;

import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class TableQuerySpy implements TableQuery {
	MethodCallRecorder MCR = new MethodCallRecorder();

	@Override
	public void addParameter(String name, Object value) {
		MCR.addCall("name", name, "value", value);
	}

	@Override
	public void addCondition(String name, Object value) {
		MCR.addCall("name", name, "value", value);
	}

	@Override
	public void setFromNo(Long fromNo) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setToNo(Long toNo) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addOrderByAsc(String column) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addOrderByDesc(String column) {
		// TODO Auto-generated method stub

	}

	@Override
	public String assembleCreateSql() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String assembleReadSql() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String assembleUpdateSql() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String assembleDeleteSql() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Object> getQueryValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String assembleCountSql() {
		// TODO Auto-generated method stub
		return null;
	}

}
