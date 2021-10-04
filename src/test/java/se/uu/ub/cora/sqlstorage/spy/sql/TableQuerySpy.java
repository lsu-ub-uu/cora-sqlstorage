package se.uu.ub.cora.sqlstorage.spy.sql;

import java.util.List;

import se.uu.ub.cora.sqldatabase.table.TableQuery;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class TableQuerySpy implements TableQuery {
	public MethodCallRecorder MCR = new MethodCallRecorder();

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
		MCR.addCall("fromNo", fromNo);
	}

	@Override
	public void setToNo(Long toNo) {
		MCR.addCall("toNo", toNo);
	}

	@Override
	public void addOrderByAsc(String column) {
		MCR.addCall("column", column);
	}

	@Override
	public void addOrderByDesc(String column) {
		MCR.addCall("column", column);
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
