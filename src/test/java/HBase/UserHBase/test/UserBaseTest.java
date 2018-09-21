package HBase.UserHBase.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import HBase.UserBase.hbase.UserDao;
import HBase.UserBase.model.User;

public class UserBaseTest {
	
	private UserDao dao;
	
	@Before
	public void constructor() throws Exception {
		dao = new UserDao("server7:2181,server8:2181,server9:2181");
		assert(dao != null);
	}
	

	@After
	public void close() throws Exception {
		dao.close();
	}
}
