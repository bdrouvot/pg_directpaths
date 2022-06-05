# pg_directpaths

## Features

Provide the following direct paths (means bypass shared buffers):

- direct path insert

### Direct path insert 

- data is written directly to the relation files, bypassing the shared buffers
- data is written directly by chunks of 8MB
- once the insert is finished new tuples are visible as if they would have been inserted through the standard insert
- new tuples are not visible if the insert is aborted
- WAL logging is done if the target relation is a logged one
- WAL logging is done by writing the Full Page Images of the new pages
- WAL logging is done by writing multiple Full Page Images in one operation

# Status

pg_directpaths is in public alpha status (so currently recommended only for experiments, testing, etc.,) but is `not currently recommended for production usage`.

## Installation

It's a module and no core changes are needed.

### Compiling

The module can be built using the standard PGXS infrastructure.  
For this to work, the ``pg_config`` program must be available in your $PATH.  
Instruction to install follows:

    # git clone https://github.com/bdrouvot/pg_directpaths.git
    # cd pg_directpaths
    # make
    # make install

## Usage

You can load it:

    postgres=# load 'pg_directpaths'

or add pg_directpaths to the `shared_preload_libraries` parameter, that way:

    shared_preload_libraries = 'pg_directpaths'

### Trigger a direct path insert

To trigger a direct path insert, the `/*+ APPEND */` hint needs to be added:

    /*+ APPEND */ insert......

# Examples

## compare the same insert time without or with the `APPEND` hint usage:

- Setup the table and indexes:

```
postgres=# create table desttable (timeid integer,insid integer,indid integer,value integer);
CREATE TABLE
postgres=# create unique index ix_desttable on desttable (timeid,indid,insid);
CREATE INDEX
postgres=# create index brin_idx_desttable  on desttable using brin (timeid);
CREATE INDEX
```

- check the plan and launch the insert without the `APPEND` hint:

```
postgres=# \timing
Timing is on.
postgres=# explain (COSTS OFF) insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
                QUERY PLAN
------------------------------------------
 Insert on desttable
   ->  Function Scan on generate_series a
(2 rows)

Time: 1.033 ms
postgres=# insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
INSERT 0 5000000
Time: 48584.243 ms (00:48.584)
```

- compare the plan and the execution time with the hint in place:

```
postgres=# truncate table desttable;
TRUNCATE TABLE
Time: 52.648 ms
postgres=# load 'pg_directpaths';
LOAD
Time: 0.385 ms
postgres=# /*+ APPEND */ explain (COSTS OFF) insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
                   QUERY PLAN
------------------------------------------------
 INSERT APPEND
   ->  Insert on desttable
         ->  Function Scan on generate_series a
(3 rows)

Time: 0.651 ms
postgres=# /*+ APPEND */ insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
INSERT 0 5000000
Time: 15475.834 ms (00:15.476)
```
 - Out of curiosity, let's compare the timing also with an unlogged table:

````
postgres=# create unlogged table nologdesttable (timeid integer,insid integer,indid integer,value integer);
CREATE TABLE
postgres=# create unique index ix_nologdesttable on nologdesttable (timeid,indid,insid);
CREATE INDEX
postgres=# create index brin_idx_nologdesttable on nologdesttable using brin (timeid);
CREATE INDEX
postgres=# insert into nologdesttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
INSERT 0 5000000
Time: 41303.905 ms (00:41.304)
````
## check the `APPEND` hint behavior within a transaction:
- If the transaction is committed 
````
postgres=# truncate table desttable;
TRUNCATE TABLE
postgres=# begin;
BEGIN
postgres=*# /*+ APPEND */ insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
INSERT 0 5000000
postgres=*# commit;
COMMIT
postgres=# select count(*) from desttable ;
  count
---------
 5000000
(1 row)
````
- If the transaction is rolled back: 
````
postgres=# truncate table desttable;
TRUNCATE TABLE
postgres=# begin;
BEGIN
postgres=*# /*+ APPEND */ insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a;
INSERT 0 5000000
postgres=*# rollback;
ROLLBACK
postgres=# select count(*) from desttable ;
 count
-------
     0
(1 row)
````
# Remarks

- the /*+ APPEND */ hint is ignored if the insert is done on a partitioned table
- direct path is working if the insert is done directly on a partition
- check constraints are ignored
- no trigger processing is performed
- an access exlusive lock is acquired on the relation
- all the relation's indexes are rebuild (even if you direct path insert a single row)
- [pg_bulkload](https://github.com/ossc-db/pg_bulkload) also provides direct path loading: part of pg_directpaths is inspired by it

# Sum up the features:

| Feature | Supported PostgreSQL version | bypass shared buffers | WAL Logging | write directly to the relation files | on partitions | rebuild indexes | explain |
|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|
| direct path insert| >=10 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

# Areas of improvement:

| Feature | on partitioned tables | incremental indexes update | fires triggers | others? |
|:-------------:|:-------------:|:-------------:|:-------------:|:-------------:|
| direct path insert| :question: | :question: | :question: | :question: |

# License

pg_directpaths is free software distributed under the PostgreSQL license.

Copyright (c) 2022, Bertrand Drouvot.