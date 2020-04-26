# Project: Data Modeling with Cassandra

The goal of this very simple project ist to develop small NoSQL-DB for analyzing / answering three specific queries. This project starts with an ETL part and then creates a small **Cassandra keyspace (3 tables)** from a directory of csv files, containing logs on user activity and metadata on the songs in a music streaming app. (Note: The data is not included in this repository.)


## Build

The project consists of one Jupyter Notebook. It runs with **Python 3.6** or higher and **Cassandra Database**.

The **database** can be installed locally or ran using Docker, which is the preferred method.

To use docker to run Cassandra, run:

``` sh
docker run -d --network host --name cassandra -e CASSANDRA_LISTEN_ADDRESS=127.0.0.1 cassandra:latest
```

And run the **Notebook** with:

``` sh
jupyter notebook
```