


DROP TABLE IF EXISTS float_range_sum_2_3_preceding_3_3_following;

CREATE TABLE float_range_sum_2_3_preceding_3_3_following(`a` Float32, `b` Float32) 
    ENGINE = CnchMergeTree() ORDER BY tuple();
    
insert into float_range_sum_2_3_preceding_3_3_following values (0,0.1) (0,0.5) (0,1.2) (0,5.3) (0,8.5) (0,14.6) (0,14.8) (0,14.9) (0,22.2) (1,3.6) (1,3.5) (1,4.6) (1,2.2) (1,6.5) (1,7.2) (1,9.1) (1,3.2) (1,6.5);

select a, sum(b) over (partition by a order by toFloat64(b) RANGE BETWEEN 2.3 PRECEDING AND 3.3 following) as res
FROM float_range_sum_2_3_preceding_3_3_following
order by a, res;

DROP TABLE float_range_sum_2_3_preceding_3_3_following;
