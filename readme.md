# Minimal Memory Querying Language
This repository provides the querying language to enable creation, maintainence, and analysis of a database that reads at maximum one row of data into memory at one time. This solution is optimal for low memory machines and is modeled similar to SAS's PDV logic. 

The highest time complexity query functions are sorting and merging. Sorting is completed through a merge sort which is O(N * log(N)) time complexity. The merging is done through a brute force single pass merging algorithm with a time complexity of O(N * M), with N being one table and M being the other.

The architecture of this language stores each individual row into its own file. This enables reading of one row at a time. Metadata regarding where each row is stored, the sort value of the table, and overall index allotments are stored in a json file in the database folder.

## Setup
Before getting started, you must be authorized to query Kaggle's API. You can read more here: https://www.kaggle.com/docs/api

To set up the database run the following three steps:
1. pip3 install -r requirements.txt
2. python3 intake_data.py
3. python3 process_data.py

This will set up the database architecture and load in two small example datasets.

## Querying
To connect to the database run the following:
* python3 query_data.py

From here any query can be placed. To get started, a simple view query is provided below:
* view(table=info)

### View
Viewing data in database.

#### Example Command Line
view(table=info, limit=20, columns=index currency, where=currency is USD)

#### Query Options
table : str
* name of table

limit : int = 10
* number of observation to limit output, use 'max' for all rows

columns : string
* column names separated by spaces (e.g. index currency)

where : str
* where to project the table, default all rows (e.g. column is value)

### Delete
Delete data in database.

#### Example Command Line
delete(table=info, where=currency is USD)
    
#### Query Options
table : str
* name of table

where : str
* where to delete the table, default all rows (e.g. column is value)

### Sort
Sorts data in selected table by selected column.

#### Example Command Line
sort(table=info, on=currency, invert=n)
    
#### Query Options
table : str
* name of table

on : str
* column to sort on

invert : str = n
* y/n for inverting the sort

### Create
Creates a new table in database

#### Example Command Line
create(table=info_new, columns = index usd)
    
#### Query Options
table : str
* name of table

columns : str
* string of columns to make new table

### Insert
Inserts data in selected table by selected column

#### Example Command Line
insert(table=info, data= oneword two|word three|word|string)
    
#### Query Options
table : str
* name of table

data : str
* string of data to load in, add in spaces using pipes '|'

### Update
Updates data in selected table by selected column

#### Example Command Line
update(table=info, where=currency is USD, to=US Dollars)
    
#### Query Options
table : str
* name of table

where : str
* string where to place data formated with column is value

to : str
* value to replace with

### Merge
Merges data on an inner join between two tables on a key

#### Example Command Line
merge(left=info, right=processed, left_on=index, right_on=index, name=info_processed)
    
#### Query Options
left : str
* table name for the left table

right : str
* table name for the right table
    
left_on : str
* key on left table

right_on : str
* key on right table

name : str
* name of new table after merge

### Group
Perform aggregate functions on given table

#### Example Command Line
group(table=processed, on=index, perform=sum, using=open, to=processed_group)
    
#### Query Options
table : str
* table to perform aggregate functions

on : str
* column to group on

perform : str
* aggregate function to perform

using : str
* column to aggregate

to : str
* table name to save to