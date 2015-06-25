# learnings

Cassandra tips:

From the CQL shell to view the keyspaces, type:

DESC KEYSPACES;

To create the keyspace "mykeyspace", at the CQL shell prompt, type:

CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

Note: Will cover the replication part later.

To use the keyspace we've just created, type:

USE mykeyspace;

Create a "users" table within the keyspace "mykeyspace" so that we can insert some data into our database:

CREATE TABLE users (user_id int,name text,primary key(user_id));

To insert the data into the newly created table,

eg.,

insert into mykeyspace.users (user_id) values (111);

To view the inserted records,

select * from mykeyspace.users;

To see the schema of all keyspaces,

DESC schema;



