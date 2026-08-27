-- Regression test: when optimize_read_in_partition_order is on and the query spans more than one
-- partition, inverted-index filtering is deferred from planning into the per-part select processor
-- (MergeTreeDataSelectExecutor::filterMarkRangesForPartByInvertedIndex). If the index drops EVERY
-- granule of a part, the processor must produce no task instead of building a reader over empty
-- mark ranges (which used to segfault in MergeTreeRangeReader::numRowsInCurrentGranule).

set enable_optimizer = 1;
set optimize_read_in_order = 1;
set optimize_read_in_partition_order = 1;
set enable_inverted_index = 1;

drop table if exists gin_empty_ranges;
create table gin_empty_ranges (ts DateTime, body String, INDEX body_idx body TYPE inverted GRANULARITY 1)
    engine = CnchMergeTree partition by toDate(ts) order by ts settings index_granularity = 2;
system stop merges gin_empty_ranges;

-- several partitions (more than there are workers, so every worker sees >1 partition and the index filter
-- is deferred); the token 'needle' lives only in the newest one, so the index drops all granules of every other part
insert into gin_empty_ranges values ('2024-06-01 10:00:00', 'alpha beta'), ('2024-06-01 11:00:00', 'gamma delta');
insert into gin_empty_ranges values ('2024-06-02 10:00:00', 'alpha beta'), ('2024-06-02 11:00:00', 'gamma delta');
insert into gin_empty_ranges values ('2024-06-03 10:00:00', 'alpha beta'), ('2024-06-03 11:00:00', 'gamma delta');
insert into gin_empty_ranges values ('2024-06-04 10:00:00', 'alpha beta'), ('2024-06-04 11:00:00', 'gamma delta');
insert into gin_empty_ranges values ('2024-06-05 10:00:00', 'alpha beta'), ('2024-06-05 11:00:00', 'gamma delta');
insert into gin_empty_ranges values ('2024-06-06 10:00:00', 'needle here'), ('2024-06-06 11:00:00', 'omega');

select 'token only in newer partition';
select toString(ts), body from gin_empty_ranges where hasToken(body, 'needle') order by ts;
select 'token only in newer partition, limit 1';
select toString(ts), body from gin_empty_ranges where hasToken(body, 'needle') order by ts limit 1;
select 'token nowhere';
select toString(ts), body from gin_empty_ranges where hasToken(body, 'zzz') order by ts;
select 'token only in newer partition, desc';
select toString(ts), body from gin_empty_ranges where hasToken(body, 'needle') order by ts desc;
select 'no index hit, all rows';
select toString(ts) from gin_empty_ranges where hasToken(body, 'alpha') or hasToken(body, 'omega') order by ts;

drop table gin_empty_ranges;

-- same shape through the late-materialize (atomic predicate) processor
drop table if exists gin_empty_ranges_lm;
create table gin_empty_ranges_lm (ts DateTime, body String, INDEX body_idx body TYPE inverted GRANULARITY 1)
    engine = CnchMergeTree partition by toDate(ts) order by ts settings index_granularity = 2, enable_late_materialize = 1;
system stop merges gin_empty_ranges_lm;
insert into gin_empty_ranges_lm values ('2024-06-01 10:00:00', 'alpha beta'), ('2024-06-01 11:00:00', 'gamma delta');
insert into gin_empty_ranges_lm values ('2024-06-02 10:00:00', 'alpha beta'), ('2024-06-02 11:00:00', 'gamma delta');
insert into gin_empty_ranges_lm values ('2024-06-03 10:00:00', 'alpha beta'), ('2024-06-03 11:00:00', 'gamma delta');
insert into gin_empty_ranges_lm values ('2024-06-04 10:00:00', 'alpha beta'), ('2024-06-04 11:00:00', 'gamma delta');
insert into gin_empty_ranges_lm values ('2024-06-05 10:00:00', 'alpha beta'), ('2024-06-05 11:00:00', 'gamma delta');
insert into gin_empty_ranges_lm values ('2024-06-06 10:00:00', 'needle here'), ('2024-06-06 11:00:00', 'omega');

select 'lm: token only in newer partition';
select toString(ts), body from gin_empty_ranges_lm where hasToken(body, 'needle') order by ts;
select 'lm: token nowhere';
select toString(ts), body from gin_empty_ranges_lm where hasToken(body, 'zzz') order by ts;

drop table gin_empty_ranges_lm;
