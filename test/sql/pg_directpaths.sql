-- non partition with indexes
create table desttable (timeid integer,insid integer,indid integer,value integer);
create unique index ix_desttable on desttable (timeid,indid,insid);
create index brin_idx_desttable  on desttable using brin (timeid);

load 'pg_directpaths';
explain (COSTS OFF) insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a order by 1,2,3;
/*+ APPEND */ explain (COSTS OFF) insert into desttable select a % 50, a % 10000, a , a from generate_series(1,5000000) a order by 1,2,3;
/*+ APPEND */ explain (COSTS OFF) insert into desttable values (30000000,10,10,10);

-- partition with indexes
create table desttablep (timeid integer,insid integer,indid integer,value integer) PARTITION BY RANGE (timeid);
create table desttablep_1 partition of desttablep for values from (1) to (2);
create table desttablep_2 partition of desttablep for values from (2) to (3);

/*+ APPEND */ explain (COSTS OFF) insert into desttablep values (1,10,10,10);
/*+ APPEND */ explain (COSTS OFF) insert into desttablep_1 values (1,10,10,10);
/*+ APPEND */ insert into desttablep_1 values (1,10,10,10);
select * from desttablep where timeid = 1;

-- triggers
create table desttable2 as select * from desttable where 1 = 2;
create table desttable3 as select * from desttable where 1 = 2;

CREATE OR REPLACE FUNCTION insert_desttable2()
RETURNS TRIGGER
AS
$$
BEGIN
RAISE NOTICE 'value of NEW.timeid : %', NEW.timeid;
INSERT INTO desttable2(timeid,insid,indid,value) VALUES(NEW.timeid * 2 ,NEW.insid,NEW.indid,NEW.value);
RETURN NEW;
END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION insert_desttable2_2()
RETURNS TRIGGER
AS
$$
BEGIN
INSERT INTO desttable2(timeid,insid,indid,value) VALUES(NEW.timeid,NEW.insid * 2,NEW.indid,NEW.value);
RETURN NEW;
END;
$$ language plpgsql;

CREATE TRIGGER trig1 BEFORE INSERT ON desttable FOR EACH ROW EXECUTE PROCEDURE insert_desttable2();
CREATE TRIGGER trig2 BEFORE INSERT ON desttable FOR EACH ROW EXECUTE PROCEDURE insert_desttable2_2();
CREATE TRIGGER trig3 AFTER INSERT ON desttable FOR EACH ROW EXECUTE PROCEDURE insert_desttable2();
CREATE TRIGGER trig4 AFTER INSERT ON desttable FOR EACH ROW EXECUTE PROCEDURE insert_desttable2_2();

CREATE OR REPLACE FUNCTION insert_desttable3()
RETURNS TRIGGER
AS
$$
BEGIN
INSERT INTO desttable3(timeid,insid,indid,value) VALUES(NEW.timeid * 2 ,NEW.insid,NEW.indid,NEW.value);
RETURN NEW;
END;
$$ language plpgsql;

CREATE TRIGGER trig5 BEFORE INSERT ON desttable2 FOR EACH ROW EXECUTE PROCEDURE insert_desttable3();
CREATE TRIGGER trig6 AFTER INSERT ON desttable2 FOR EACH ROW EXECUTE PROCEDURE insert_desttable3();

insert into desttable values (1,10,10,10);
select * from desttable;
select * from desttable2;
select * from desttable3;

truncate table desttable;
truncate table desttable2;
truncate table desttable3;
/*+ APPEND */ insert into desttable values (1,10,10,10);
select * from desttable2;
select * from desttable3;

-- check constraint
create table checkcons (a int, b int, c int check (c > 1));
insert into checkcons values (1, 1, 1);
select * from checkcons;
/*+ APPEND */ insert into checkcons values (1, 1, 1);
select * from checkcons;

-- toast
SET default_toast_compression = 'pglz';
create table toasttest(descr text, cnt int DEFAULT 0, f1 text, f2 text);
insert INTO toasttest(descr, f1, f2) VALUES('two-toasted', repeat('1234567890',30000), repeat('1234567890',50000));
select relname from pg_class where oid = (select reltoastrelid from pg_class where relname='toasttest');\gset
select substring(chunk_data::text, 1, 10)  from pg_toast.:relname;

drop table toasttest;
create table toasttest(descr text, cnt int DEFAULT 0, f1 text, f2 text);
/*+ APPEND */ explain insert INTO toasttest(descr, f1, f2) VALUES('two-toasted', repeat('1234567890',30000), repeat('1234567890',50000));
/*+ APPEND */ insert INTO toasttest(descr, f1, f2) VALUES('two-toasted', repeat('1234567890',30000), repeat('1234567890',50000));
select relname from pg_class where oid = (select reltoastrelid from pg_class where relname='toasttest');\gset
select substring(chunk_data::text, 1, 10)  from pg_toast.:relname;
