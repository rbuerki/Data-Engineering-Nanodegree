# DEMOS: Advantages of Denormalized Data for Analytics

2 cool notebook excercices in Raw SQL.

## Notebooks

### Notebook 1: Advantages of Denormalized Data Schemas for Analytics

The purpose of this notebook is to demonstrate the advantages of a Star Schema over 3NF for analytics purposes. First we query on the provided 3NF schema, then we create facts and dimensions for a an analytics-friendly Star schema.

Most interesting thing is the setup of the fact and dimension tables, especially:

- creating an optimized primary key for the date dimension
- setting references in the fact table
- double setting of _ids in DimTables

### Notebook 2: Further Optimization with Cubes and Grouping-Sets

See notebook for details. Self-explanatory.

## Data

All the database tables in this demo are based on public database samples and transformations:

- [Sakila](https://dev.mysql.com/doc/sakila/en/sakila-structure.html) is a sample database created by `MySql`
- [Pagila](https://github.com/devrimgunduz/pagila) ist the `postgresql` version of it that we use in this excercice
- The facts and dimension tables design is based on O'Reilly's public [dimensional modelling tutorial](http://archive.oreilly.com/oreillyschool/courses/dba3/index.html)

(Note: The data is not included in this repository.)

## Build

This project runs with the same setup as project 1, but uses **Raw SQL**, and _no_ psycopg2.
