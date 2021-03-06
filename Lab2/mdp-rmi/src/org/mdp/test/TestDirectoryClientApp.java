package org.mdp.test;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;

import org.mdp.RMIUtils;
import org.mdp.dir.User;
import org.mdp.dir.UserDirectoryServer;
import org.mdp.dir.UserDirectoryStub;

/**
 * This is a simple client example for UserDirectory*.
 * 
 * First the remote registry on the server is found.
 * 
 * Next the stub is retrieved from the registry. The registry is simply
 * a map from stub-names (String) to stubs (an interface extending Remote).
 * 
 * Once the stub is retrieved by the client, it can invoke
 * remote methods to be executed on the server. 
 * 
 * @author Aidan
 *
 */
public class TestDirectoryClientApp {
	// TODO you need to replace this if not using localhost.
	// Details will be given in the class.
	//
	// If you are doing this at home, you can start
	// a directory on your local machine and leave these
	// details as they are.
	public static final String DIR_HOST = "192.80.24.222";
	public static final int DIR_PORT = RMIUtils.DEFAULT_REG_PORT;
	
	//TODO replace with your details here
	public static final String USER_NAME = "aidhog";
	public static final String USER_FULL_NAME = "Aidan Hogan";
	public static final String USER_HOST = "10.0.114.126";
	public static final int USER_PORT = 1985;
	
	/**
	 * An example to test central the directory.
	 * 
	 * Will connect to directory, upload some user details,
	 * get list of users, remove user, get list of users again.
	 * 
	 * @param args
	 * @throws AccessException
	 * @throws RemoteException
	 * @throws NotBoundException
	 */
	public static void main(String[] args) throws AccessException, RemoteException, NotBoundException{
		
		// first need to connect to the remote registry on the given
		// IP and port
		Registry registry = LocateRegistry.getRegistry(DIR_HOST, DIR_PORT);
		
		// then need to find the interface we're looking for 
		UserDirectoryStub stub = (UserDirectoryStub) registry.lookup(UserDirectoryServer.DEFAULT_KEY);
		
		// now we can use the stub to call remote methods!!
		Map<String,User> users = stub.getDirectory();
		System.out.println("Current users in directory ...");
		System.out.println(users.toString());
		
		User u = new User(USER_NAME, USER_FULL_NAME, USER_HOST, USER_PORT);
		
		System.out.println("Adding self ...");
		stub.createUser(u);
			
		System.out.println("Current users in directory (2) ...");
		users = stub.getDirectory();
		System.out.println(users.toString());
		
		System.out.println("Removing self ...");
		stub.removeUserWithName(USER_NAME);
		
		System.out.println("Current users in directory (3) ...");
		users = stub.getDirectory();
		System.out.println(users.toString());
	}
}
