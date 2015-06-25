package com.learning.cassandra;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraImpl implements CassandraWrapper {

	Session session;

	public void readData(Session session) {

		try {
			this.session = session;

			final ResultSet rs = session.execute("select * from users");
			final List<Row> row = rs.all();

			for (Row r : row) {
				System.out.println("User Id: " + r.getInt("user_id"));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeData(Session session, int user_id) {
		try {
			this.session = session;

			final ResultSet result = session.execute(
					"insert into mykeyspace.users (user_id) values (?)",
					user_id);
			final List<Row> row = result.all();

			for (Row r : row) {
				System.out.println("User Id : " + r.getInt("user_id"));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
