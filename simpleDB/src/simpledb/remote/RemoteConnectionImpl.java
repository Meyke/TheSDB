package simpledb.remote;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteConnection.
 * @author Edward Sciore
 */
@SuppressWarnings("serial") 
class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
	private Transaction tx;

	/**
	 * Creates a remote connection
	 * and begins a new transaction for it.
	 * @throws RemoteException
	 */
	RemoteConnectionImpl() throws RemoteException {
		tx = new Transaction();
	}

	/**
	 * Creates a new RemoteStatement for this connection.
	 * @see simpledb.remote.RemoteConnection#createStatement()
	 */
	public RemoteStatement createStatement() throws RemoteException {
		return new RemoteStatementImpl(this);
	}

	/**
	 * Closes the connection.
	 * The current transaction is committed.
	 * @see simpledb.remote.RemoteConnection#close()
	 */
	public void close() throws RemoteException {
		tx.commit();
	}

	// The following methods are used by the server-side classes.

	/**
	 * Returns the transaction currently associated with
	 * this connection.
	 * @return the transaction associated with this connection
	 */
	Transaction getTransaction() {  
		return tx;
	}

	/**
	 * Commits the current transaction,
	 * and begins a new one.
	 */
	void commit() {
		tx.commit();
		System.out.println(stampaStatistiche());
		SimpleDB.fileMgr().resetBlockStats(); //perchè voglio azzerare le statistiche a ogni interrogazione
		tx = new Transaction();
	}

	private String stampaStatistiche() {
		return SimpleDB.fileMgr().getBlockStats().toString();
	}

	/**
	 * Rolls back the current transaction,
	 * and begins a new one.
	 */
	void rollback() {
		tx.rollback();
		tx = new Transaction();
	}
}

