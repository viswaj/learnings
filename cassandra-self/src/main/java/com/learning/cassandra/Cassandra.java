package com.learning.cassandra;

import com.datastax.driver.core.Session;

public class Cassandra {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		final CassandraCli client = new CassandraCli();
		final String node = args.length > 0 ? args[0] : "localhost";
		final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
		final String keyspace = args.length > 2 ? args[2] : "mykeyspace";

		client.connect(node, port, keyspace);
		Session session = client.getSession();

		CassandraImpl cl = new CassandraImpl();

		cl.readData(session);
		cl.writeData(session, 999);

		client.close();

	}

}
