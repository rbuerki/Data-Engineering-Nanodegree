# Project: Data Modeling with Cassandra

In this very simple project a small NoSQL-DB is created to analyze / answer three specific queries. This project starts with an ETL part and then creates a  **Cassandra keyspace (3 tables)** from a local directory of csv files, containing logs on user activity and metadata on the songs in a music streaming app. (Note: The data is not included in this repository.)

The main purpose of this exercice is to emphasize the difference between NoSQL an RDBMS. NoSQL does _not_ allow table JOINs, GROUP BYs, subqueries and the like. So you have to organize your tables carefully around specific queries that have to be formulated before you construct the DB.

## Build

The project consists of one Jupyter Notebook. It runs with **Python 3.6** or higher and a **Cassandra Database**.

The **database** can be installed locally or ran using Docker, which is the preferred method.

To use docker to run Cassandra, run:

``` sh
docker run -d --network host --name cassandra -e CASSANDRA_LISTEN_ADDRESS=127.0.0.1 cassandra:latest
```

And run the **Notebook** with:

``` sh
jupyter notebook
```
