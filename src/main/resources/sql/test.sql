WITH U AS(
    SELECT DISTINCT(user_id) as user_id FROM model.d_latetime_orders orders
    WHERE order_status = 'Sent' AND orders.pay_date >='2016-11-01' and orders.pay_date <='2016-11-30'
),  J AS(
      SELECT orders.user_id, rank as original_rank, p_obs FROM U orders
        LEFT JOIN agg.latetime_feature_20161101_20161130_predicttest client ON orders.user_id = client.user_id),
    C AS(
      SELECT user_id,
        (CASE
         WHEN original_rank is NULL THEN 27511
         ELSE original_rank
         END) AS rank,
        (CASE
         WHEN p_obs is NULL THEN 0
         ELSE p_obs
         END) AS p_obs
      FROM J)

SELECT grp, sum(p_obs) AS p_obs_sum FROM (SELECT ntile(10) OVER ( ORDER BY rank) AS grp, p_obs, user_id
                                          FROM C WHERE rank <= 50000) AS tmp
GROUP BY grp ORDER BY grp;


SELECT sum(p_obs) from agg.latetime_feature_20161101_20161130_predicttest where rank <=2589;
SELECT sum(p_obs) from agg.latetime_feature_20161101_20161130_predictclient where rank <=2589;

SELECT count(*) from agg.latetime_feature_20161101_20161130_predicttest;
SELECT count(*) from agg.latetime_feature_20161101_20161130_predictclient;



SELECT grp, sum(p_obs) AS p_obs_sum FROM (SELECT ntile(100) OVER ( ORDER BY score DESC) AS grp, p_obs FROM agg.latetime_feature_20161101_20161130_predicttest WHERE rank <= 50000) AS tmp GROUP BY grp ORDER BY grp



with mindates as (
    select a.user_id, min(pay_date) as min_date from model.d_tutuanna_orders a inner join agg.tutuanna_wazi11_20160701_20160913_predicttest b
        on a.user_id=b.user_id group by a.user_id
)
select count(distinct orders.user_id) from model.d_tutuanna_orders orders
  inner JOIN agg.tutuanna_wazi11_20160701_20160913_predicttest agg on orders.user_id = agg.user_id
  left join mindates m on orders.user_id=m.user_id
where product_id in ( '1829' ) AND order_status = 'Sent'
      AND rank <= 50000 and pay_date between '2016-07-01' and '2016-09-13' and m.min_date < '2016-07-01';




create table model.d_tutuannafull_user_min_order_date as
select a.user_id, min(pay_date) as min_date from model.d_tutuannafull_orders a group by a.user_id;


create INDEX  bolome_user_id_min_date on model.d_bolome_user_min_order_date (user_id, min_date)
create INDEX  bolome_min_date on model.d_bolome_user_min_order_date (min_date)



select count(distinct orders.user_id) from
  (model.d_latetime_orders orders inner JOIN agg.latetime_feature_20161101_20161130_predicttest agg on orders.user_id = agg.user_id)
  INNER JOIN model.d_latetime_user_min_order_date m ON orders.user_id = m.user_id
where product_id in ( '1' , '2' , '3' , '4' , '5' ) AND order_status = 'Sent' AND rank <= 50000
      AND min_date < '2016-11-01' and pay_date between '2016-11-01' and '2016-11-30'


select count(distinct orders.user_id) from
  (model.d_latetime_orders orders inner JOIN agg.latetime_feature_20161101_20161130_predicttest agg on orders.user_id = agg.user_id)

where product_id in ( '1' , '2' , '3' , '4' , '5' ) AND order_status = 'Sent' AND rank <= 50000
      and pay_date between '2016-11-01' and '2016-11-30'


select count(distinct orders.user_id ) from model.d_latetime_orders orders INNER JOIN model.d_latetime_user_min_order_date m ON orders.user_id = m.user_id
where product_id in ( '1' , '2' , '3' , '4' , '5' ) AND order_status = 'Sent'
      AND min_date < '2016-11-01' and pay_date between '2016-11-01' and '2016-11-30'



select count(distinct orders.user_id) from (model.d_tutuanna_orders orders inner JOIN agg.tutuanna_wenxiong333_20170401_20170430_predicttest agg on orders.user_id = agg.user_id) INNER JOIN model.d_tutuanna_user_min_order_date m ON orders.user_id = m.user_id where product_id in ( '184' , '185' , '187' , '188' , '190' ) AND order_status = 'Sent' AND rank <= 500000 AND min_date < '2017-04-01' and pay_date between '2017-04-01' and '2017-04-30'


SELECT count(DISTINCT orders.user_id) FROM model.d_tutuanna_orders orders
  INNER JOIN model.d_tutuanna_user_min_order_date m on orders.user_id = m.user_id
where order_status = 'Sent' and pay_date < '2017-04-01' and min_date < '2017-04-01';
=>95861

SELECT count(DISTINCT orders.user_id) FROM model.d_tutuanna_orders orders
where order_status = 'Sent' and pay_date < '2017-04-01'


UPDATE conf.latetime_itemmarketing_firstfilter SET filter_name='500' where job_id = 'latetime_feature2_20161101_20161115'