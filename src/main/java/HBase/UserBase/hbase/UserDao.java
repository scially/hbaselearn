package HBase.UserBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import HBase.UserBase.model.User;

public class UserDao {
	
	private static final byte[] TABLE_NAME = Bytes.toBytes("users");
	private static final byte[] INFO_FAMILY = Bytes.toBytes("info");
	private static final byte[] NAME_COL = Bytes.toBytes("name");
	private static final byte[] EMAIL_COL = Bytes.toBytes("email");
	private static final byte[] PASSWORD_COL = Bytes.toBytes("password");
	private static final byte[] USER_COL = Bytes.toBytes("user");
	
	private Connection con;
	private Admin admin;
	private Configuration config;
	
	public UserDao(String zookeeper) throws IOException {
		config = HBaseConfiguration.create();
		config.set(HConstants.ZOOKEEPER_QUORUM, zookeeper);
		con = ConnectionFactory.createConnection(config);
		admin = con.getAdmin();
	}
	
	public boolean tableExist() {
		boolean exist = false;
		try {
			exist = admin.tableExists(TableName.valueOf(TABLE_NAME));
		}  catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return exist;
	}
	
	public boolean createTable() throws Exception {
		if(tableExist()) return false;
		
		HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		ht.addFamily(new HColumnDescriptor(INFO_FAMILY));
		try {
			admin.createTable(ht);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public void deleteTable() {
		if(!tableExist()) return;
		
		try {
			admin.disableTable(TableName.valueOf(TABLE_NAME));
			admin.deleteTable(TableName.valueOf(TABLE_NAME));
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public Put mkPut(String user, String name, String email, String password, String news) {
		Put put = new Put(Bytes.toBytes(user));
		put.addColumn(INFO_FAMILY, EMAIL_COL, Bytes.toBytes(email));
		put.addColumn(INFO_FAMILY, PASSWORD_COL, Bytes.toBytes(password));
		put.addColumn(INFO_FAMILY, USER_COL, Bytes.toBytes(user));
		put.addColumn(INFO_FAMILY, NAME_COL, Bytes.toBytes(name));
		return put;
	}
	
	public Get mkGet(String user) {
		Get get = new Get(Bytes.toBytes(user));
		get.addFamily(INFO_FAMILY);
		return get;
	}
	public Delete mkDel(String user) {
		
		Delete del = new Delete(Bytes.toBytes(user));
		return del;
	}
	
	public void putData(String user, String name, String email, String password, String news) throws Exception {
		Put put = mkPut(user, name, email, password, news);
		Table table = con.getTable(TableName.valueOf(TABLE_NAME));
		table.put(put);
		
	}
	public User getData(String user) throws Exception {
		Get get = mkGet(user);
		Table table = con.getTable(TableName.valueOf(TABLE_NAME));
		Result res = table.get(get);
		return new HUser(res);
		
	}
	
	public List<User> getDatas() throws Exception{
		Scan scan = new Scan();
		Table table = con.getTable(TableName.valueOf(TABLE_NAME));
		ResultScanner rs = table.getScanner(scan);
		List<User> users = new ArrayList<>();
		for(Result r : rs) {
			users.add(new HUser(r));
		}
		
		return users;
	}
	
	public void delData(String name) throws Exception {
		Delete del = mkDel(name);
		
		Table table = con.getTable(TableName.valueOf(TABLE_NAME));
		table.delete(del);
	}
	public void close() {
		try {
			admin.close();
			con.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
		}
		
	}
	
	
	private static class HUser extends User{
		private HUser(Result res) {
			this(res.getValue(INFO_FAMILY, NAME_COL),
				 res.getValue(INFO_FAMILY, USER_COL),
				 res.getValue(INFO_FAMILY, EMAIL_COL),
				 res.getValue(INFO_FAMILY, PASSWORD_COL));
		}
		
		private HUser(byte[] user, byte[] name, byte[] email, 
				byte[] password) {
			setUser(Bytes.toString(user));
			setName(Bytes.toString(name));
			setEmail(Bytes.toString(email));
			setPassword(Bytes.toString(password));
		}
	}
	
}
