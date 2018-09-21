package HBase.UserBase;

import java.util.List;

import org.apache.log4j.Logger;

import HBase.UserBase.hbase.UserDao;
import HBase.UserBase.model.User;


public class UsersMain {
	private static final Logger log = Logger.getLogger(UsersMain.class);
	public static final String usage =
			"usertool action ...\n" +
			"  help - print this message and exit.\n" +
			"  add user name email password - add a new user.\n" +
			"  get user - retrieve a specific user.\n" +
			"  list - list all users.\n";

	public static void main(String[] args) throws Exception {	  
	    	      if (args.length == 0 || "help".equals(args[0])) {    	    	  
	    	            System.out.println(usage);            
	    	            return;       
	    	      }
	    	      
	    	      UserDao dao = new UserDao("server7:2181,server8:2181,server9:2181");	  
	    	      if ("get".equals(args[0])) {
	    	           log.debug(String.format("Getting user %s", args[1]));
	    	           User u  = dao.getData(args[1]);
	    	           System.out.println("Successfully get " + u);
	    	      }
	    	      if ("add".equals(args[0])) {
	    	    	  log.debug("Adding user...");
	    	          dao.putData(args[1], args[2], args[3], args[4], "");
	    	          User u = dao.getData(args[1]);
	    	          System.out.println("Successfully added user " + u);
	    	        }
	    	       if ("list".equals(args[0])) {
	    	          List<User> users = dao.getDatas();
	    	          log.info(String.format("Found %s users.", users.size()));
	    	          for(User u : users) {
	    	            System.out.println(u);
	    	          }
	    	        }
	      }
}
