package se.uu.ub.cora.postgres;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class InitialContextSpy extends InitialContext {
	public DataSource ds;
	public String name;

	public int noOfLookups = 0;

	public InitialContextSpy() throws NamingException {
		super();
		ds = new DataSourceSpy();
	}

	@Override
	public Object lookup(String name) throws NamingException {
		noOfLookups++;
		this.name = name;
		return ds;
	}

}
