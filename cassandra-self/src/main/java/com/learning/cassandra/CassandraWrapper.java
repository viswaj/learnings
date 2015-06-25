package com.learning.cassandra;

import com.datastax.driver.core.Session;

public interface CassandraWrapper {

	public void readData(Session session);
	public void writeData(Session session,int user_id);
	
}
