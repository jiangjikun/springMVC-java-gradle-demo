-- 求所有时间下 TOP N
WITH product_count AS (
    SELECT
      product_id,
      sum(quantity) AS count
    FROM model.d_bolome_orders
    WHERE order_status = 'Sent'
    GROUP BY product_id
    ORDER BY count DESC
), first_n AS (
    SELECT
      row_number()
      OVER () AS orders,
      product_id,
      count
    FROM product_count
    LIMIT 5
), others AS (
    SELECT
      6                AS orders,
      'others' :: TEXT AS product_id,
      sum(tmp.count)   AS count
    FROM (SELECT
            product_id,
            count
          FROM product_count
          OFFSET 5) AS tmp
), result AS (
  SELECT
    orders,
    product_id,
    count
  FROM first_n
  UNION ALL
  SELECT
    orders,
    product_id,
    count
  FROM others
) SELECT
    orders,
    product_id,
    count
  FROM result
  ORDER BY orders;



-- 求每个月的 TOP N
WITH product_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM') AS month,
      product_id,
      sum(quantity)                     AS count
    FROM model.d_bolome_orders
    WHERE order_status = 'Sent'
    GROUP BY product_id, month
), partition_product_by_month AS (
    SELECT
      row_number()
      OVER (
        PARTITION BY month
        ORDER BY count DESC ) AS orders,
      month,
      product_id,
      count
    FROM product_count
), first_n AS (
    SELECT *
    FROM partition_product_by_month
    WHERE orders < 6
), others AS (
    SELECT
      6              AS orders,
      month,
      '其他产品' :: TEXT AS product_id,
      sum(count)     AS count
    FROM partition_product_by_month
    WHERE orders >= 6
    GROUP BY month
), result AS (
  SELECT
    orders,
    month,
    product_id,
    count
  FROM first_n
  UNION ALL
  SELECT
    orders,
    month,
    product_id,
    count
  FROM others
) SELECT
    orders,
    month,
    product_id,
    count
  FROM result
  ORDER BY month;

-- 求每个月的 TOP N 和 求 product_name
WITH product_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM') AS month,
      product_id,
      count(*)                     AS num
    FROM model.d_bolome_orders
    WHERE order_status = 'Sent'
    AND pay_date IS NOT NULL
    AND product_id IS NOT NULL
    GROUP BY product_id, month
), partition_product_by_month AS (
    SELECT
      row_number()
      OVER (
        PARTITION BY month
        ORDER BY num DESC ) AS orders,
      month,
      product_id,
      num
    FROM product_count
), first_n AS (
    SELECT *
    FROM partition_product_by_month
    WHERE orders < 6
), others AS (
    SELECT
      6              AS orders,
      month,
      '其他产品' :: TEXT AS product_id,
      sum(num)       AS num
    FROM partition_product_by_month
    WHERE orders >= 6
    GROUP BY month
), src_result AS (
  SELECT
    orders,
    month,
    product_id,
    num
  FROM first_n
  UNION ALL
  SELECT
    orders,
    month,
    product_id,
    num
  FROM others
), products AS (
    SELECT
      product_id :: TEXT,
      product_name
    FROM model.d_bolome_products
), result AS (
    SELECT
      orders,
      month,
      products.product_name,
      num
    FROM src_result
      LEFT JOIN products ON src_result.product_id = products.product_id
)
SELECT
  orders,
  month,
  CASE WHEN result.product_name IS NULL
    THEN '其他产品'
  ELSE result.product_name END AS product_name,
  num
FROM result
ORDER BY month, orders;

------------------------------------------------------------------------------------------------------------------------


-- 顾客客单价按月分布
WITH customer_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM')                       AS month,
      user_id,
      SUM(pay_amt :: DOUBLE PRECISION),
      COUNT(DISTINCT order_id),
      SUM(pay_amt :: DOUBLE PRECISION) / COUNT(DISTINCT order_id) AS single_user_order_avg
    FROM model.d_latetime_orders
    WHERE order_status = 'Sent'
    GROUP BY month, user_id)
  --   SELECT * FROM customer_count;
  , month_order_avg AS (
    SELECT
      month,
      SUM(single_user_order_avg) / COUNT(user_id) AS total_user_order_avg
    FROM customer_count
    GROUP BY month
    ORDER BY month
) SELECT *
  FROM month_order_avg;


-- 顾客购买量按月分布
WITH customer_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM') AS month,
      --       按类别分
      COUNT(DISTINCT product_id)                     AS num,
      user_id
    FROM model.d_latetime_orders
    WHERE order_status = 'Sent'
    GROUP BY month, user_id
)
  -- SELECT * FROM customer_count;
  , partition_product_by_month AS (
    SELECT
      row_number()
      OVER (
        PARTITION BY month
        ORDER BY num ) AS orders,
      month,
      num,
      count(num)       AS count
    FROM customer_count
    GROUP BY month, num
)
  --   SELECT * FROM partition_product_by_month;
  , first_n AS (
    SELECT
      orders,
      month,
      num   AS category,
      count AS num
    FROM partition_product_by_month
    WHERE num < 3
), others AS (
    SELECT
      3          AS orders,
      month,
      3         AS category,
      sum(count) AS num
    FROM partition_product_by_month
    WHERE num >= 3
    GROUP BY month
), result AS (
  SELECT
    orders,
    month,
    category,
    num
  FROM first_n
  UNION ALL
  SELECT
    orders,
    month,
    category,
    num
  FROM others
) SELECT
    month,
    category,
    num
  FROM result
  ORDER BY month, orders;


-- 月销量和月增长
WITH product_count AS (
    SELECT
      substr(pay_date :: TEXT, 0, 8)    AS month,
      product_id,
      SUM(quantity :: DOUBLE PRECISION) AS sales_num
    FROM d_data_source_orders
    WHERE order_status = 'Sent'
    GROUP BY product_id, month
), partition_product_by_id AS (
    SELECT
      row_number()
      OVER (
        PARTITION BY product_id
        ORDER BY month ) AS orders,
      month,
      product_id,
      sales_num
    FROM product_count
)
  -- SELECT * FROM partition_product_by_id;
  , product_pp AS (
    SELECT
      (partition_product_by_id.orders + 1) AS orders,
      month                                AS pp_month,
      product_id,
      sales_num                            AS pp_sales_num
    FROM partition_product_by_id
)
  --   SELECT * FROM product_pp;
  , product_all AS (
    SELECT
      partition_product_by_id.orders,
      partition_product_by_id.month,
      partition_product_by_id.product_id,
      partition_product_by_id.sales_num,
      product_pp.pp_month,
      product_pp.pp_sales_num,
      product_pp.product_id AS pp_product_id
    FROM partition_product_by_id
      LEFT JOIN product_pp ON partition_product_by_id.orders = product_pp.orders
                              AND partition_product_by_id.product_id = product_pp.product_id
)
  --   SELECT * FROM product_all;
  , product_vspp AS (
    SELECT
      orders,
      month,
      product_all.product_id,
      d_bolome_products.product_name,
      sales_num,
      (sales_num - pp_sales_num) / pp_sales_num AS vspp
    FROM product_all
      LEFT JOIN model.d_bolome_products ON product_all.product_id = d_bolome_products.product_id :: TEXT
)
SELECT
  month,
  product_name,
  sales_num,
  vspp
FROM product_vspp
ORDER BY product_id, month;

------------------------------------------------------------------------------------------------------------------------

-- 用户客单价计算： 用户购买产品所花的钱的和 / 购买产品数
-- 用户沉默天数计算： 数据库最大的一天 - 用户最后一笔订单的时间
-- 用户月购买频次计算: 用户总订单数 / 数据总的月数 ((end_day - start_day ) / 30)


------------- 用户特征表
CREATE VIEW model.d_bolome_users_trait AS WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt::FLOAT) AS total_money,
      MAX(pay_date) AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count) AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v FROM month_count)) AS frequency
    FROM user_orders
) SELECT * FROM users_trait;
COMMENT ON VIEW model.d_bolome_users_trait IS 'bolome 用户特征';
COMMENT ON COLUMN model.d_bolome_users_trait.user_id IS '用户 ID';
COMMENT ON COLUMN model.d_bolome_users_trait.order_avg IS '用户客单价';
COMMENT ON COLUMN model.d_bolome_users_trait.activity IS '用户沉默天数';
COMMENT ON COLUMN model.d_bolome_users_trait.frequency IS '用户月购买频次';

------------------------------------------------------------

-- 对整体用户 根据客单价 按人数平均分组 计算 用户沉默天数 用户月购买频次
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), users_trait_partition AS (
    SELECT
      user_id,
      order_avg,
      activity,
      frequency,
      NTILE(5)
      OVER (
        ORDER BY order_avg ) AS user_group
    FROM users_trait
), result AS (
    SELECT
      user_group,
      MIN(order_avg) AS left_range,
      MAX(order_avg) AS right_range,
      AVG(activity)  AS activity_avg,
      AVG(frequency) AS frequency_avg,
      COUNT(*)       AS count
    FROM users_trait_partition
    GROUP BY user_group
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    activity_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;


-- 对整体用户 根据用户沉默天数 按人数平均分组 计算 用户客单价 用户月购买频次
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), users_trait_partition AS (
    SELECT
      user_id,
      order_avg,
      activity,
      frequency,
      NTILE(5)
      OVER (
        ORDER BY activity ) AS user_group
    FROM users_trait
), result AS (
    SELECT
      user_group,
      MIN(activity)  AS left_range,
      MAX(activity)  AS right_range,
      AVG(order_avg) AS order_avg_avg,
      AVG(frequency) AS frequency_avg,
      COUNT(*)       AS count
    FROM users_trait_partition
    GROUP BY user_group
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;


-- 对整体用户 根据用户月购买频次 按人数平均分组 计算 用户客单价 用户沉默天数
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt::FLOAT) AS total_money,
      MAX(pay_date) AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count) AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v FROM month_count)) AS frequency
    FROM user_orders
), users_trait_partition AS (
    SELECT
      user_id,
      order_avg,
      activity,
      frequency,
      NTILE(5) OVER(ORDER BY frequency) AS user_group
    FROM users_trait
), result AS (
    SELECT
      user_group,
      MIN(frequency) AS left_range,
      MAX(frequency) AS right_range,
      AVG(order_avg) AS order_avg_avg,
      AVG(activity) AS activity_avg,
      COUNT(*) AS count
    FROM users_trait_partition GROUP BY user_group
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    activity_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

------------------------------------------------------------


-- 对整体用户 根据客单价 按值平均分组 计算 用户沉默天数 用户月购买频次
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt::FLOAT) AS total_money,
      MAX(pay_date) AS user_max_pay_date
    FROM model.d_latetime_orders
    GROUP BY user_id
) , max_date AS (
    SELECT MAX(pay_date) AS v FROM model.d_latetime_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v FROM model.d_latetime_orders
), users_trait_before AS (
    SELECT
      user_id,
      (total_money / order_count) AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v FROM month_count)) AS frequency
    FROM user_orders
), order_avg_trait(min_order_avg, max_order_avg) AS (
  -- 平均值 +/- 3 * 标准差
  SELECT (AVG(order_avg) - 3 * stddev_pop(order_avg)), (AVG(order_avg) + 3 * stddev_pop(order_avg)) FROM users_trait_before
), users_trait AS (
  SELECT * FROM users_trait_before AS u WHERE u.order_avg BETWEEN (SELECT min_order_avg FROM order_avg_trait) AND (SELECT max_order_avg FROM order_avg_trait)
), order_avg(max, min) AS (
    SELECT
      MAX(order_avg) + 0.0001,
      MIN(order_avg)
    FROM users_trait
), order_avg_range AS (
    SELECT
      ((SELECT min
        FROM order_avg) + ((SELECT max
                            FROM order_avg) - (SELECT min
                                               FROM order_avg)) * (i - 1) / 5.0) AS left_range,
      ((SELECT min
        FROM order_avg) + ((SELECT max
                            FROM order_avg) - (SELECT min
                                               FROM order_avg)) * i / 5.0)       AS right_range
    FROM generate_series(1, 5) AS t(i)
), result AS (
    SELECT
      a.left_range,
      a.right_range,
      AVG(t.activity)  AS activity_avg,
      AVG(t.frequency) AS frequency_avg,
      count(*)
    FROM users_trait AS t
      RIGHT JOIN order_avg_range AS a
        ON t.order_avg BETWEEN a.left_range AND a.right_range
    GROUP BY a.left_range, a.right_range
), users_trait_drop_left AS ( -- 被丢弃的左边的特征
    SELECT * FROM users_trait_before AS u WHERE u.order_avg < (SELECT min_order_avg FROM order_avg_trait)
), users_trait_drop_right AS ( -- 被丢弃的右边的特征
    SELECT * FROM users_trait_before AS u WHERE u.order_avg > (SELECT max_order_avg FROM order_avg_trait)
), left_result AS (
    SELECT
      MIN(order_avg) AS left_range,
      MAX(order_avg) AS right_range,
      AVG(activity)  AS activity_avg,
      AVG(frequency) AS frequency_avg,
      count(*)
    FROM users_trait_drop_left
), right_result AS (
    SELECT
      MIN(order_avg) AS left_range,
      MAX(order_avg) AS right_range,
      AVG(activity)  AS activity_avg,
      AVG(frequency) AS frequency_avg,
      count(*)
    FROM users_trait_drop_right
), result_include_drop AS (
  SELECT *
  FROM left_result
       WHERE left_range IS NOT NULL
       UNION
       SELECT *
       FROM result
            UNION
            SELECT *
            FROM right_result
            WHERE left_range IS NOT NULL
) SELECT
     ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
     left_range :: NUMERIC(10, 2),
     right_range :: NUMERIC(10, 2),
     activity_avg,
     frequency_avg,
     count
   FROM result_include_drop
   ORDER BY left_range, right_range;

-- 对整体用户 根据用户沉默天数 按值平均分组 计算 用户客单价 用户月购买频次
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt::FLOAT) AS total_money,
      MAX(pay_date) AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count) AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v FROM month_count)) AS frequency
    FROM user_orders
), activity(max, min) AS (
    SELECT
      MAX(activity) + 0.0001,
      MIN(frequency)
    FROM users_trait
), activity_range AS (
    SELECT
      ((SELECT min
        FROM activity) + ((SELECT max
                           FROM activity) - (SELECT min
                                             FROM activity)) * (i - 1) / 5.0) AS left_range,
      ((SELECT min
        FROM activity) + ((SELECT max
                           FROM activity) - (SELECT min
                                             FROM activity)) * i / 5.0)       AS right_range
    FROM generate_series(1, 5) AS t(i)
), result AS (
    SELECT
      a.left_range,
      a.right_range,
      AVG(t.order_avg) AS order_avg_avg,
      AVG(t.frequency) AS frequency_avg,
      count(*)
    FROM users_trait AS t
      RIGHT JOIN activity_range AS a
        ON t.activity BETWEEN a.left_range AND a.right_range
    GROUP BY a.left_range, a.right_range
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

-- 对整体用户 根据用户月购买频次 按值平均分组 计算 用户客单价 用户沉默天数
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt::FLOAT) AS total_money,
      MAX(pay_date) AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count) AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v FROM month_count)) AS frequency
    FROM user_orders
), frequency(max, min) AS (
    SELECT
      MAX(frequency) + 0.0001,
      MIN(frequency)
    FROM users_trait
), activity_range AS (
    SELECT
      ((SELECT min
        FROM frequency) + ((SELECT max
                            FROM frequency) - (SELECT min
                                               FROM frequency)) * (i - 1) / 5.0) AS left_range,
      ((SELECT min
        FROM frequency) + ((SELECT max
                            FROM frequency) - (SELECT min
                                               FROM frequency)) * i / 5.0)       AS right_range
    FROM generate_series(1, 5) AS t(i)
), result AS (
    SELECT
      a.left_range,
      a.right_range,
      AVG(t.order_avg) AS order_avg_avg,
      AVG(t.activity)  AS activity_avg,
      count(*)
    FROM users_trait AS t
      RIGHT JOIN activity_range AS a
        ON t.frequency BETWEEN a.left_range AND a.right_range
    GROUP BY a.left_range, a.right_range
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    activity_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

------------------------------------------------------------


------------------------------------------------------------

-- 对 job 相关的用户 根据客单价 按人数平均分组 计算 用户沉默天数 用户月购买频次
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
-- ), products AS (
--     SELECT products
--     FROM conf.tutuanna_itemmarketing_job
--     WHERE name = 'tutuanna_wazi1_20160831_20160831'
), users AS (
    SELECT DISTINCT s.user_id
    FROM d_data_source_orders AS s
      INNER JOIN model.d_tutuanna_user_min_order_date AS m
    ON s.user_id = m.user_id
    AND m.min_date < '2017-02-18'::DATE
      INNER JOIN agg.tutuanna_31test_20170301_20170331_predictclient AS p
        ON s.user_id = p.user_id
      AND p.rank <= 1000
      INNER JOIN model.d_tutuanna_client AS c
      ON s.user_id = c.user_id
      AND c.address_1 IN (
        SELECT value :: TEXT
              FROM json_array_elements('[23, 23]':: JSON))
    WHERE s.pay_date BETWEEN '2' AND '2'
--     WHERE s.product_id IN (
--       SELECT value :: TEXT
--       FROM json_array_elements(replace((SELECT products
--                                         FROM products), '"', '') :: JSON))
), user_orders AS (
    SELECT
      O.user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders O RIGHT OUTER JOIN users ON O.user_id = users.user_id
    GROUP BY O.user_id
), max_date AS (
  -- 参数
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
  -- max min 改为传参
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), order_avg_partition AS (
    SELECT
      user_id,
      order_avg,
      activity,
      frequency,
      NTILE(5)
      OVER (
        ORDER BY order_avg ) AS user_group
    FROM users_trait
), result AS (
    SELECT
      MIN(order_avg) AS left_range,
      MAX(order_avg) AS right_range,
      AVG(activity)  AS activity_avg,
      AVG(frequency) AS frequency_avg,
      COUNT(*)       AS count
    FROM order_avg_partition
    GROUP BY user_group
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    activity_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

-- 对 job 相关的用户 根据用户沉默天数 按人数平均分组 计算 用户客单价 用户月购买频次 -Deprecated
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), products AS (
    SELECT products
    FROM conf.tutuanna_itemmarketing_job
    WHERE name = 'tutuanna_wazi1_20160831_20160831'
), users AS (
    SELECT DISTINCT s.user_id
    FROM d_data_source_orders AS s
      INNER JOIN model.d_tutuanna_user_min_order_date AS m
    ON s.user_id = m.user_id
    AND m.min_date < '2017-02-18'::DATE
    WHERE s.product_id IN (
      SELECT value :: TEXT
      FROM json_array_elements(replace((SELECT products
                                        FROM products), '"', '') :: JSON))
), user_orders AS (
    SELECT
      O.user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders O RIGHT OUTER JOIN users ON O.user_id = users.user_id
    GROUP BY O.user_id
), max_date AS (
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), activity_partition AS (
    SELECT
      user_id,
      order_avg,
      activity,
      frequency,
      NTILE(5)
      OVER (
        ORDER BY activity ) AS user_group
    FROM users_trait
), result AS (
    SELECT
      MIN(activity)  AS left_range,
      MAX(activity)  AS right_range,
      AVG(order_avg) AS order_avg_avg,
      AVG(frequency) AS frequency_avg,
      COUNT(*)       AS count
    FROM activity_partition
    GROUP BY user_group
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

-- 对 job 相关的用户 根据用户月购买频次 按人数平均分组 计算 用户客单价 用户沉默天数  -Deprecated
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), products AS (
    SELECT products
    FROM conf.tutuanna_itemmarketing_job
    WHERE name = 'tutuanna_wazi1_20160831_20160831'
), users AS (
    SELECT DISTINCT s.user_id
    FROM d_data_source_orders AS s
      INNER JOIN model.d_tutuanna_user_min_order_date AS m
    ON s.user_id = m.user_id
    AND m.min_date < '2017-02-18'::DATE
    WHERE s.product_id IN (
      SELECT value :: TEXT
      FROM json_array_elements(replace((SELECT products
                                        FROM products), '"', '') :: JSON))
), user_orders AS (
    SELECT
      O.user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders O RIGHT OUTER JOIN users ON O.user_id = users.user_id
    GROUP BY O.user_id
), max_date AS (
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), frequency_partition AS (
    SELECT
      user_id,
      order_avg,
      activity,
      frequency,
      NTILE(5)
      OVER (
        ORDER BY frequency ) AS user_group
    FROM users_trait
), result AS (
    SELECT
      MIN(frequency) AS left_range,
      MAX(frequency) AS right_range,
      AVG(order_avg) AS order_avg_avg,
      AVG(activity)  AS activity_avg,
      COUNT(*)       AS count
    FROM frequency_partition
    GROUP BY user_group
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    activity_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

------------------------------------------------------------

-- 对 job 相关的用户 根据客单价 按值平均分组 计算 用户沉默天数 用户月购买频次  -Deprecated
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), products AS (
    SELECT products
    FROM conf.tutuanna_itemmarketing_job
    WHERE name = 'tutuanna_wazi1_20160831_20160831'
), users AS (
    SELECT DISTINCT s.user_id
    FROM d_data_source_orders AS s
      INNER JOIN model.d_tutuanna_user_min_order_date AS m
    ON s.user_id = m.user_id
    AND m.min_date < '2017-02-18'::DATE
    WHERE s.product_id IN (
      SELECT value :: TEXT
      FROM json_array_elements(replace((SELECT products
                                        FROM products), '"', '') :: JSON))
), user_orders AS (
    SELECT
      O.user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders O RIGHT OUTER JOIN users ON O.user_id = users.user_id
    GROUP BY O.user_id
), max_date AS (
  -- 参数
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
  -- 参数 最大 最小 日期
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), order_avg(max, min) AS (
    SELECT
      MAX(order_avg) + 0.0001,
      MIN(order_avg)
    FROM users_trait
), order_avg_range AS (
    SELECT
      ((SELECT min
        FROM order_avg) + ((SELECT max
                            FROM order_avg) - (SELECT min
                                               FROM order_avg)) * (i - 1) / 5.0) AS left_range,
      ((SELECT min
        FROM order_avg) + ((SELECT max
                            FROM order_avg) - (SELECT min
                                               FROM order_avg)) * i / 5.0)       AS right_range
    FROM generate_series(1, 5) AS t(i)
), result AS (
    SELECT
      a.left_range,
      a.right_range,
      AVG(t.activity)  AS activity_avg,
      AVG(t.frequency) AS frequency_avg,
      count(*)
    FROM users_trait AS t
      RIGHT JOIN order_avg_range AS a
        ON t.order_avg BETWEEN a.left_range AND a.right_range
    GROUP BY a.left_range, a.right_range
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    activity_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

-- 对 job 相关的用户 根据用户沉默天数 按值平均分组 计算 用户客单价 用户月购买频次  -Deprecated
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), products AS (
    SELECT products
    FROM conf.tutuanna_itemmarketing_job
    WHERE name = 'tutuanna_wazi1_20160831_20160831'
), users AS (
    SELECT DISTINCT s.user_id
    FROM d_data_source_orders AS s
      INNER JOIN model.d_tutuanna_user_min_order_date AS m
    ON s.user_id = m.user_id
    AND m.min_date < '2017-02-18'::DATE
    WHERE s.product_id IN (
      SELECT value :: TEXT
      FROM json_array_elements(replace((SELECT products
                                        FROM products), '"', '') :: JSON))
), user_orders AS (
    SELECT
      O.user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders O RIGHT OUTER JOIN users ON O.user_id = users.user_id
    GROUP BY O.user_id
), max_date AS (
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), activity(max, min) AS (
    SELECT
      MAX(activity) + 0.0001,
      MIN(activity)
    FROM users_trait
), activity_range AS (
    SELECT
      ((SELECT min
        FROM activity) + ((SELECT max
                           FROM activity) - (SELECT min
                                             FROM activity)) * (i - 1) / 5.0) AS left_range,
      ((SELECT min
        FROM activity) + ((SELECT max
                           FROM activity) - (SELECT min
                                             FROM activity)) * i / 5.0)       AS right_range
    FROM generate_series(1, 5) AS t(i)
), result AS (
    SELECT
      a.left_range,
      a.right_range,
      AVG(t.order_avg) AS order_avg_avg,
      AVG(t.frequency) AS frequency_avg,
      count(*)
    FROM users_trait AS t
      RIGHT JOIN activity_range AS a
        ON t.activity BETWEEN a.left_range AND a.right_range
    GROUP BY a.left_range, a.right_range
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    frequency_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

-- 对 job 相关的用户 根据用户月购买频次 按值平均分组 计算 用户客单价 用户沉默天数 -Deprecated
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), products AS (
    SELECT products
    FROM conf.tutuanna_itemmarketing_job
    WHERE name = 'tutuanna_wazi1_20160831_20160831'
), users AS (
    SELECT DISTINCT s.user_id
    FROM d_data_source_orders AS s
      INNER JOIN model.d_tutuanna_user_min_order_date AS m
    ON s.user_id = m.user_id
    AND m.min_date < '2017-02-18'::DATE
    WHERE s.product_id IN (
      SELECT value :: TEXT
      FROM json_array_elements(replace((SELECT products
                                        FROM products), '"', '') :: JSON))
), user_orders AS (
    SELECT
      O.user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt :: FLOAT) AS total_money,
      MAX(pay_date)         AS user_max_pay_date
    FROM d_data_source_orders O RIGHT OUTER JOIN users ON O.user_id = users.user_id
    GROUP BY O.user_id
), max_date AS (
    SELECT MAX(pay_date) AS v
    FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v
    FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count)                                 AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v
                      FROM month_count))                          AS frequency
    FROM user_orders
), frequency(max, min) AS (
    SELECT
      MAX(frequency) + 0.0001,
      MIN(frequency)
    FROM users_trait
), frequency_range AS (
    SELECT
      ((SELECT min
        FROM frequency) + ((SELECT max
                            FROM frequency) - (SELECT min
                                               FROM frequency)) * (i - 1) / 5.0) AS left_range,
      ((SELECT min
        FROM frequency) + ((SELECT max
                            FROM frequency) - (SELECT min
                                               FROM frequency)) * i / 5.0)       AS right_range
    FROM generate_series(1, 5) AS t(i)
), result AS (
    SELECT
      a.left_range,
      a.right_range,
      AVG(t.order_avg) AS order_avg_avg,
      AVG(t.activity)  AS activity_avg,
      count(*)
    FROM users_trait AS t
      RIGHT JOIN frequency_range AS a
        ON t.frequency BETWEEN a.left_range AND a.right_range
    GROUP BY a.left_range, a.right_range
) SELECT
    ((left_range :: NUMERIC(10, 2)) :: TEXT || '-' || (right_range :: NUMERIC(10, 2)) :: TEXT) AS range,
    left_range :: NUMERIC(10, 2),
    right_range :: NUMERIC(10, 2),
    order_avg_avg,
    activity_avg,
    count
  FROM result
  ORDER BY left_range, right_range;

------------------------------------------------------------




------------------------------



SELECT date '2002-10-01' - date '2001-09-28';




SELECT AGE(now(), '1970-01-01'::TIMESTAMP);

SELECT EXTRACT (YEAR FROM AGE('2016-06-30'::DATE , '2014-03-01'::DATE )) * 12 + EXTRACT (MONTHS FROM AGE('2016-06-30'::DATE , '2014-03-01'::DATE ));


SELECT EXTRACT (DAY FROM AGE('2016-06-30'::DATE , '2014-03-01'::DATE ));

SELECT value::text FROM json_array_elements('["1","2"]');
SELECT * FROM model.d_bolome_orders WHERE product_id IN ('2');

SELECT products FROM conf.bolome_itemmarketing_job WHERE name = 'bolome_test_20170508_20170606';

SELECT value::text FROM json_array_elements(replace((SELECT products FROM conf.bolome_itemmarketing_job WHERE name = 'bolome_test_20170508_20170606'),'"','')::json);

SELECT user_id FROM model.d_bolome_orders WHERE product_id IN (SELECT value::text FROM json_array_elements(replace((SELECT products FROM conf.bolome_itemmarketing_job WHERE name = 'bolome_caizhuangtest_20170508_20170606'),'"','')::json));

-- 根据 job_id 求 相关用户
WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      product_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), products AS (
    SELECT products FROM conf.latetime_itemmarketing_job WHERE name = 'latetime_feature_20161101_20161130'
), users AS (
    SELECT user_id FROM model.d_latetime_orders
    WHERE product_id IN (
      SELECT value::text FROM json_array_elements(replace((SELECT products FROM conf.latetime_itemmarketing_job WHERE name = 'latetime_feature_20161101_20161130'),'"','')::json))
) SELECT * FROM users;

------------------------------


------------------------------------------------------------------------------------------------------------------------

-- 根据 JobId 的开始日期(start_date)和结束日期(finish_date)，推算 预测的最大日期(max_pay_date)和最小日期(min_pay_date)的方法
-- 其中 max_date 为数据库中的最大日期, min_date 为数据库中的最小日期
-- if (start_date > max_date):
--         max_pay_date = max_date
-- else:
--         max_pay_date = start_date - 1 day
-- min_pay_date = max_pay_date - 1 year
-- min_pay_date = min_date < min_pay_date ? min_pay_date : min_date

------------------------------------------------------------------------------------------------------------------------


WITH d_data_source_orders AS (
    SELECT
      user_id,
      order_id,
      pay_date,
      pay_amt
    FROM model.d_tutuanna_orders
  WHERE user_id IS NOT NULL
  AND order_id IS NOT NULL
  AND pay_date IS NOT NULL
  AND pay_amt IS NOT NULL
  AND order_status = 'Sent'
), user_orders AS (
    SELECT
      user_id,
      COUNT(DISTINCT order_id)              AS order_count,
      SUM(pay_amt::FLOAT) AS total_money,
      MAX(pay_date) AS user_max_pay_date
    FROM d_data_source_orders
    GROUP BY user_id
), max_date AS (
    SELECT MAX(pay_date) AS v FROM d_data_source_orders
), month_count AS (
    SELECT ((MAX(pay_date) - MIN(pay_date)) / 30::FLOAT) AS v FROM d_data_source_orders
), users_trait AS (
    SELECT
      user_id,
      (total_money / order_count) AS order_avg,
      ((SELECT v FROM max_date) - user_max_pay_date) AS activity,
      (order_count / (SELECT v FROM month_count)) AS frequency
    FROM user_orders
), order_avg_ntile AS (
    SELECT
      order_avg,
      NTILE(100)
      OVER (
        ORDER BY order_avg DESC ) AS ntile
    FROM users_trait
), order_avg_num AS (
    SELECT
      ROW_NUMBER()
      OVER (
        PARTITION BY ntile
        ORDER BY ntile, order_avg ) AS num,
      *
    FROM order_avg_ntile
), activity_ntile AS (
    SELECT
      activity,
      NTILE(100)
      OVER (
        ORDER BY activity ) AS ntile
    FROM users_trait
), activity_num AS (
    SELECT
      ROW_NUMBER()
      OVER (
        PARTITION BY ntile
        ORDER BY ntile, activity ) AS num,
      *
    FROM activity_ntile
), frequency_ntile AS (
    SELECT
      frequency,
      NTILE(100)
      OVER (
        ORDER BY frequency DESC ) AS ntile
    FROM users_trait
), frequency_num AS (
    SELECT
      ROW_NUMBER()
      OVER (
        PARTITION BY ntile
        ORDER BY ntile, frequency ) AS num,
      *
    FROM frequency_ntile
), sorted_trait AS (
    SELECT
      o.order_avg,
      a.activity,
      f.frequency,
      o.ntile
    FROM order_avg_num AS o
      JOIN activity_num AS a ON o.ntile = a.ntile AND o.num = a.num
      JOIN frequency_num AS f ON o.ntile = f.ntile AND o.num = f.num
), result AS (
    SELECT
      AVG(order_avg) AS order_avg_avg,
      AVG(activity) AS activity_avg,
      AVG(frequency) AS frequency_avg,
      ntile,
      count(*) AS count
    FROM sorted_trait
    GROUP BY ntile
) SELECT * FROM result ORDER BY ntile;







































------------------------------------------------------------------------------------------------------------------------
















------------------------------------------------------------------------------------------------------------------------
