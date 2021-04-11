package org.mdp.im;

import java.rmi.RemoteException;

import org.mdp.dir.User;
import org.mdp.dir.UserDirectoryServer;

public class InstantMessagingServer implements InstantMessagingStub {

	// default key we will use for the registry
	public static String DEFAULT_KEY = UserDirectoryServer.class.getSimpleName();
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6682365848634470441L;

	public long message(User from, String msg) throws RemoteException {
		//TODO here you need to implement the messaging server :)
		// return the current time: System.currentTimeMillis()

		System.out.println(from.getUsername());
		System.out.println(msg);

		return System.currentTimeMillis();
	}
}
