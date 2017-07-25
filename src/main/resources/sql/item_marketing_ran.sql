
-- 顾客客单价按月分布
WITH customer_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM')                                AS month,
      user_id,
      SUM(pay_amt :: DOUBLE PRECISION),
      COUNT(DISTINCT order_id),
      SUM(pay_amt :: DOUBLE PRECISION) / COUNT(DISTINCT order_id) AS single_user_order_avg
    FROM model.d_latetime_orders
    WHERE order_status = 'Sent' AND pay_date IS NOT NULL AND user_id IS NOT NULL
          AND pay_amt IS NOT NULL
    GROUP BY month, user_id)
-- 单个顾客客单价按月
  --   SELECT * FROM customer_count;
  , month_order_avg AS (
    SELECT
      month,
      SUM(single_user_order_avg) / COUNT(user_id) AS total_user_order_avg
    FROM customer_count
    GROUP BY month
    ORDER BY month
)
--   分均顾客客单价
SELECT * FROM month_order_avg;


-- 顾客购买量按月分布
WITH customer_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM') AS month,
      --       按类别分
      COUNT(DISTINCT product_id)   AS num
    FROM model.d_latetime_orders
    WHERE order_status = 'Sent' AND pay_date IS NOT NULL
          AND product_id IS NOT NULL
    GROUP BY month, user_id
)
-- 不同用户在当月购买数量
--       SELECT * FROM customer_count;
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
-- 统计购买１个产品有多少顾客，２个产品顾客数以及３个.．．
--         SELECT * FROM partition_product_by_month;
  , first_n AS (
    SELECT
      month,
      num   AS category,
      count AS num
    FROM partition_product_by_month
    WHERE num < 3
)
--   １个产品和２个产品
  , others AS (
    SELECT
      month,
      3          AS category,
      count AS num
    FROM partition_product_by_month
    WHERE num >= 3
)
-- ３个产品及以上
  --   SELECT * FROM others;
  , result AS (
  SELECT
    month,
    category,
    num
  FROM first_n
  UNION ALL
  SELECT
    month,
    category,
    num
  FROM others
) SELECT
    month,
    category,
    num
  FROM result
  ORDER BY month, category;


-- 月销量和月增长
WITH product_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM')    AS month,
      SUM(quantity :: DOUBLE PRECISION) AS sales_num
    FROM MODEL.d_bolome_orders
    WHERE order_status = 'Sent' AND product_id = '4432'
          --     WHERE product_id IN (SELECT product_id :: TEXT FROM model.d_bolome_products WHERE category_1 =
          --                                                                              '个人护理') AND order_status = 'Sent'
          --     WHERE order_status = 'Sent' AND product_id = ANY(ARRAY['4432', '4431'])
          AND pay_date IS NOT NULL AND quantity IS NOT NULL
    GROUP BY month
    ORDER BY month
)
--   求每个月销售量
  , partition_product_by_id AS (
    SELECT
      ( month || '-01') :: DATE AS month,
      sales_num
    FROM product_count
)
-- ((( month || '-01') :: DATE ) + INTERVAL '1 MONTH')
--   SELECT * FROM partition_product_by_id;
  , product_pp AS (
    SELECT
      TO_CHAR((month  + INTERVAL '1 MONTH') , 'YYYY-MM')       AS month,
      sales_num                            AS pp_sales_num
    FROM
      partition_product_by_id
)
-- 加一个月以便join
--   SELECT  * FROM product_pp;
  , product_all AS (
    SELECT
      TO_CHAR(partition_product_by_id.month, 'YYYY-MM') AS month,
      partition_product_by_id.sales_num,
      product_pp.pp_sales_num
    FROM partition_product_by_id
      LEFT JOIN product_pp ON  TO_CHAR(partition_product_by_id.month, 'YYYY-MM') = product_pp.month
)
--  当月销量和上月销量合并到一行
--  SELECT * FROM product_all;
  , product_vspp AS (
    SELECT
      month,
      sales_num,
      (sales_num - pp_sales_num) / pp_sales_num AS vspp
    FROM product_all
)
SELECT
  month,
  sales_num,
  vspp
FROM product_vspp
ORDER BY month;

-- 月销量半年

SELECT MAX(pay_date) FROM model.d_bolome_orders;

SELECT
  pay_date,
  SUM(quantity :: DOUBLE PRECISION) AS sales_num
FROM MODEL.d_bolome_orders
WHERE
   pay_date >'2016-01-01' :: DATE AND pay_date <= '2016-06-30' :: DATE AND
  order_status = 'Sent' AND product_id = '4432'
      --     WHERE product_id IN (SELECT product_id :: TEXT FROM model.d_bolome_products WHERE category_1 =
      --                                                                              '个人护理') AND order_status = 'Sent'
--           WHERE order_status = 'Sent' AND product_id = ANY(ARRAY['4432', '4431'])
      AND pay_date IS NOT NULL AND quantity IS NOT NULL
GROUP BY pay_date
ORDER BY pay_date;

-- 月活率
WITH month_before_count AS (
    SELECT
      '2013-04' :: TEXT AS month,
      COUNT(DISTINCT (user_id)) AS before_total_customer_count
    FROM model.d_latetime_orders
    WHERE pay_date < ((('2013-04' || '-01') :: DATE ) + INTERVAL '1 MONTH')
          AND order_status = 'Sent' AND user_id IS NOT NULL
)
--   之前月份顾客数（包括当前）
  , month_count AS (
  SELECT
    TO_CHAR(pay_date, 'YYYY-MM') AS month,
    COUNT(DISTINCT (user_id)) AS month_customer_count
  FROM model.d_latetime_orders
  WHERE TO_CHAR(pay_date, 'YYYY-MM') = '2013-04'
        AND order_status = 'Sent' AND user_id IS NOT NULL
  GROUP BY month
)
--   该月份顾客数
  , month_info AS (
  SELECT
    month_before_count.month,
    month_before_count.before_total_customer_count,
    month_count.month_customer_count
  FROM month_before_count
    JOIN month_count ON month_before_count.month = month_count.month
)
--   Join 2张表
  , month_live_customer_rate AS (
  SELECT
    (month_customer_count :: DOUBLE PRECISION / before_total_customer_count :: DOUBLE PRECISION )  AS month_live_rate
      FROM month_info
)
SELECT * FROM month_live_customer_rate;

-- 产品关联购买
WITH order_not_null AS (
  SELECT order_id, product_id, quantity, pay_amt
  FROM model.d_latetime_orders
  WHERE order_status = 'Sent'
        AND quantity IS NOT NULL
        AND pay_amt IS NOT NULL
        AND order_id IS NOT NULL
        AND product_id IS NOT NULL
), get_order_by_product AS (
    SELECT * FROM order_not_null
    WHERE product_id = '225'
)
-- 找到包含所选商品的订单
  , get_other_products_by_order AS (
    SELECT * FROM order_not_null
    WHERE order_id IN (SELECT order_id FROM get_order_by_product)
)
--   找到订单中包含商品的订单
  , sum_by_product AS (
    SELECT
      product_id,
      COUNT(*) AS order_count,
      SUM(quantity :: INTEGER) AS quantity_count,
      SUM(pay_amt :: DOUBLE PRECISION) AS pay_amt_count
    FROM get_other_products_by_order
    GROUP BY product_id
)
--   到每个商品的总数量，金额以及订单个数
  , product_id_name AS (
    SELECT
      aa.product_id, bb.product_name, aa.order_count, aa.quantity_count, aa.pay_amt_count
    FROM sum_by_product AS aa,
    model.d_latetime_products AS bb
      WHERE aa.product_id = bb.product_id ::TEXT
    ORDER BY order_count DESC
)
SELECT * FROM product_id_name;


-- 测试
SELECT * FROM model.d_latetime_orders
WHERE pay_date >'2016-01-01' :: DATE AND pay_date <= '2016-06-30' :: DATE AND order_status = 'Sent' AND
       product_id IN (SELECT product_id :: TEXT FROM model.d_latetime_products
       WHERE category_1 = '迟到时光旗舰店' AND category_2 = '迟到时光旗舰店');

SELECT * FROM model.d_bolome_orders
WHERE pay_date = '2015-12-04' :: DATE AND order_status = 'Sent' AND
       product_id IN (SELECT product_id :: TEXT FROM model.d_latetime_products
       WHERE category_1 = '迟到时光旗舰店');

SELECT * FROM model.d_bolome_orders
WHERE pay_date = '2015-12-04' :: DATE;

SELECT pay_date, SUM(quantity :: DOUBLE PRECISION) AS sales_num FROM model.d_bolome_orders WHERE order_status = 'Sent' AND pay_date > '2015-12-03' :: DATE AND pay_date <= '2016-04-05' :: DATE AND pay_date IS NOT NULL AND quantity IS NOT NULL GROUP BY pay_date ORDER BY pay_date;

SELECT MAX(pay_date) FROM model.d_bolome_orders;
SELECT MIN(pay_date) FROM model.d_bolome_orders;


WITH customer_month_count AS (
    SELECT
      TO_CHAR(pay_date, 'YYYY-MM') AS month,
    COUNT(DISTINCT(user_id))
    FROM model.d_latetime_orders
    WHERE order_status = 'Sent' AND pay_date IS NOT NULL
          AND product_id IS NOT NULL
    GROUP BY month
), customer_month_before_count AS (
   SELECT
      TO_CHAR(pay_date, 'YYYY-MM') AS month,
      COUNT(DISTINCT(user_id))
    FROM model.d_latetime_orders
    WHERE order_status = 'Sent' AND pay_date IS NOT NULL
          AND product_id IS NOT NULL
--           AND  TO_CHAR(pay_date, 'YYYY-MM') <
)
    SELECT * FROM customer_month_count;

SELECT COUNT(DISTINCT(user_id)) FROM model.d_latetime_orders;

SELECT ((('2013-04' || '-01') :: DATE ) + INTERVAL '1 MONTH');


-- WHERE product_id = '2915';




WITH get_order_by_product AS (
    SELECT * FROM model.d_bolome_orders
    WHERE product_id = '2915'
), get_order_by_other_porducet AS (
    SELECT * FROM model.d_bolome_orders
    WHERE product_id = '70'
)
  SELECT * FROM get_order_by_other_porducet
    INNER JOIN  get_order_by_product
    ON get_order_by_other_porducet.order_id = get_order_by_product.order_id;
