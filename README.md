# Project 2 - Data Modeling with Cassandra

A startup called Sparkify hired you as a Data Engineer to help their Analytics team to get useful insights from event data. Initially, all the user activity is stored in a dataset folder consisting of CSV files.

Your mission is to collect the data, model it to optimize queries on songplay and load the denormalized data in an Apache Cassandra keyspace.

This project is part of the **Udacity Data Engineer nanodegree program** as a second assignment related to Data Modeling with Cassandra course.

----

## Data Engineering Tasks

The main goal is to extract, transform and load data from a directory containing CSV files:

1. event_data: A folder containing a subset of event data acquired from the interaction of users with the music streaming app. It consists of CSV files partitioned by day relative to 11/2018.

To achieve this, it is proposed the following tasks:

### Modeling your NoSQL database or Apache Cassandra database

**Design tables to answer the queries outlined in the project template**:

1. Write Apache Cassandra `CREATE KEYSPACE` and `SET KEYSPACE` statements;
1. Develop your `CREATE` statement for each of the tables to address each question;
1. Load the data with `INSERT` statement for each of the tables;
1. Include `IF NOT EXISTS` clauses in your `CREATE` statements to create tables only if the tables do not already exist. We recommend you also include `DROP TABLE` statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline;
1. Test by running the proper select statements with the correct WHERE clause.


**Build ETL Pipeline**:

1. Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
1. Make necessary edits to Part II of the notebook template to include Apache Cassandra `CREATE` and `INSERT` statements to load processed records into relevant tables in your data model
1. Test by running `SELECT` statements after running the queries on your database

----

## Running Instructions

To achieve the goals described above, you should go through the `ETL.ipynb` notebook provided. It will create/recreate the tables and apply ETL routines to load data into the Apache Cassandra Keyspace. Then it will answer the tree proposed questions from the assignment.

----


## Data Model and Business Questions

The Cassandra data model follows directly from what is requested in the business questions:

### Question 1

* Give me the `artist`, `song` title and song's `length` in the music app history that was heard during  `sessionId = 338`, and `itemInSession  = 4`

Designed following physical model:

|<td colspan="3">**song_by_session**| | |
|-|-|-|
| PT_KEY | session_id INT |  |
| CL_KEY | item_in_session INT |  ASC |
|  | artist TEXT |  |
|  | song TEXT |  |
|  | length FLOAT |  |


This model can spread evenly the data through the partition key `session_id`. I included `item_in_session` as a clustering column so it can only identify each row and can be used by the query.
    
Answer:
    
| |artist|song|length|
|-|-|-|-|
|0|Faithless|Music Matters (Mark Knight Dub)|495.307312|
    
### Question 2

* Give me only the following: name of `artist`, `song` (sorted by `item_in_session`) and `user` (first and last name) for `user_id = 10`, `session_id = 182`

Designed the following physical model:

|<td colspan="3">**song_by_user**| | |
|-|-|-|
| PT_KEY | user_id INT |  |
| PT_KEY | song_id INT |  |
| CL_KEY | item_in_session INT |  ASC |
|  | artist TEXT |  |
|  | song TEXT |  |
|  | user TEXT |  |


This model is composed by two partition keys `user_id`, `session_id`. I included `item_in_session` as a clustering column so it can be used to sort the query.
    
Answer:
    
| |	|artist|	song|	user|
|-|-|-|-|-|
|0|	|Down To The Bone|	Keep On Keepin' On|	Sylvie Cruz|
|1|	|Three Drives|	Greece 2000|	Sylvie Cruz|
|2|	|Sebastien Tellier|	Kilometer|	Sylvie Cruz|
|3|	|Lonnie Gordon|	Catch You Baby (Steve Pitron & Max Sanna Radio...|	Sylvie Cruz|
    
    
### Question 3

* Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

Designed the following physical model:

|<td colspan="3">**user_by_song**| | |
|-|-|-|
| PT_KEY | song TEXT |  |
| CL_KEY | user TEXT | ASC |


This model is composed by a partition key `song_id`. I included `user` as a clustering column so the row could be uniquely identified.
    
Answer:
    
|	|user|
|-|-|
|0	|Jacqueline Lynch|
|1	|Sara Johnson|
|2	|Tegan Levine|