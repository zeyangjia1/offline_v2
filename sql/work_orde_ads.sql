use worker_oder;
set hive.exec.mode.local.auto=true;
CREATE TABLE worker_oder.ads_Sales_volume
(
    product_id              STRING COMMENT '商品 ID',
    days                    bigint comment '1,7,30天',
    shop_id                 STRING COMMENT '店铺 ID',
    sales_amount            DECIMAL(10, 2) COMMENT '销售额',
    sales_quantity          INT COMMENT '销量',
    payment_conversion_rate string COMMENT '支付转化率'
) COMMENT '商品销售额销量支付转化表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
select *
from ads_Sales_volume;
insert into table ads_Sales_volume
select product_id,
       days,
       shop_id,
       sum(amount)                                                as amount,
       sum(quantity)                                              as quantity,
       --支付转化率=支付买家数/商品访客数
       concat(sum(if(is_paid = true, 1, 0)) / sum(visitors), "%") as lv
from dws_a
         LATERAL VIEW explode(array(1, 7, 30)) days AS days
WHERE date_format(to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))), 'yyyy-MM-dd')
    >= date_add('2025-03-27', -days + 1)
group by product_id, shop_id, days;



create table worker_oder.ads_Sales_ranking
(
    product_id        STRING COMMENT '商品 ID',
    days              bigint comment '1,7,30天',
    shop_id           STRING COMMENT '店铺 ID',
    sales_amount_rank INT COMMENT '销售额排名'

) COMMENT '商品销售额排行表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
insert into ads_Sales_ranking
select product_id,
       days,
       shop_id,
       sum(amount) as amount
from dws_a
         LATERAL VIEW explode(array(1, 7, 30)) days AS days
WHERE date_format(to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))), 'yyyy-MM-dd')
    >= date_add('2025-03-27', -days + 1)
group by product_id, shop_id, days
order by amount desc;



create table worker_oder.ads_sales_quantity_rank
(
    product_id          STRING COMMENT '商品 ID',
    days                bigint comment '1,7,30天',
    shop_id             STRING COMMENT '店铺 ID',
    sales_quantity_rank INT COMMENT '销量排名'

) COMMENT '销量排名表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
insert into table ads_sales_quantity_rank
select product_id,
       days,
       shop_id,
       sum(quantity) as sales_quantity_rank
from dws_a
         LATERAL VIEW explode(array(1, 7, 30)) days AS days
WHERE date_format(to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))), 'yyyy-MM-dd')
    >= date_add('2025-03-27', -days + 1)
group by product_id, shop_id, days
order by sales_quantity_rank desc;



create table worker_oder.ads_top10_Search
(
    days        bigint comment '1,7,30天',
    shop_id     STRING COMMENT '店铺 ID',
    visitor_num INT COMMENT '访客数',
    Search      string comment '搜索词'

) COMMENT 'top10搜索词'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");

insert into table ads_top10_Search
SELECT days,
       shop_id,
       visitors_num,
       keyword_num
FROM (SELECT days,
             shop_id,
             SUM(visitors)                                                                             AS visitors_num,
             COUNT(keyword)                                                                            AS keyword_num,
             ROW_NUMBER() OVER (PARTITION BY shop_id, days ORDER BY SUM(visitors) DESC, COUNT(*) DESC) AS a
      FROM dws_a LATERAL VIEW explode(array(1, 7, 30)) days_table AS days
      WHERE to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))) >=
          date_add('2025-03-27', -days + 1)
      GROUP BY days, shop_id) w
WHERE w.a <= 10;



create table worker_oder.ads_top10_Flow_rate
(
    product_id              STRING COMMENT '商品 ID',
    days                    bigint comment '1,7,30天',
    shop_id                 STRING COMMENT '店铺 ID',
    source                  STRING COMMENT '流量来源',
    visitors                INT COMMENT '访客数',
    payment_conversion_rate string COMMENT '支付转化率'
) COMMENT 'top10流量表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");


INSERT INTO TABLE ads_top10_Flow_rate
SELECT product_id,
       days,
       shop_id,
       source,
       visitors_num,
       concat(round(paid_ratio * 100, 2), '%') as lv
FROM (SELECT days,
             shop_id,
             product_id,
             source,
             SUM(visitors)                                                                                              AS visitors_num,
             SUM(IF(is_paid = true, 1, 0)) / SUM(visitors)                                                              AS paid_ratio,
             ROW_NUMBER() OVER (PARTITION BY shop_id, product_id, days ORDER BY SUM(visitors) DESC, COUNT(source) DESC) AS a
      FROM dws_a
               LATERAL VIEW explode(array(1, 7, 30)) days_table AS days
      WHERE to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))) >=
          date_add('2025-03-27', -days + 1)
      GROUP BY days,
          shop_id,
          product_id,
          source) w
WHERE w.a <= 10;



create table worker_oder.top5_information
(
    product_id STRING COMMENT '商品 ID',
    days       bigint comment '1,7,30天',
    shop_id    STRING COMMENT '店铺 ID',
    zf_count   bigint comment '支付件数',
    nums       bigint comment '当前库存'

) COMMENT 'top5信息表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");


insert into table top5_information
SELECT product_id,
       days,
       shop_id,
       zf_count,
       nums
FROM (SELECT product_id,
             days,
             shop_id,
             SUM(IF(is_paid = true, 1, 0))                                                                          AS zf_count,
             MAX(stock)                                                                                             AS nums,
             ROW_NUMBER() OVER (PARTITION BY shop_id, product_id, days ORDER BY SUM(IF(is_paid = true, 1, 0)) DESC) AS a
      FROM dws_a
               LATERAL VIEW explode(array(1, 7, 30)) days_table AS days
      WHERE to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))) >=
          date_add('2025-03-27', -days + 1)
      GROUP BY product_id,
          days,
          shop_id) w
WHERE w.a <= 5;



select *
from top5_information;


-- 价格力商品表
CREATE TABLE worker_oder.ads_price_force_product
(
    dt                    DATE COMMENT '统计日期',
    product_id            STRING COMMENT '商品 ID',
    shop_id               STRING COMMENT '店铺 ID',
    days                  bigint comment '1,7,30天',
    price_force_level     STRING COMMENT '价格力等级（优秀/良好/较差）',
    price_force_warning   STRING COMMENT '价格力预警信息',
    product_force_warning STRING COMMENT '商品力预警信息'
)
    COMMENT '价格力商品数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");



insert into table ads_price_force_product
select "2025-04-01" as dt,
       product_id,
       shop_id,
       days,
       case
           when avg(price) >= 1000 then "优秀"
           when avg(price) <= 1000 and avg(price) >= 500 then "良好"
           else "较差"
           end      as price_force_level,
       case
           when sum(amount) <= 2000 then "销售额低于2000价格力竞争不足"
           end      as price_force_warning,
       case
           when (sum(if(is_paid = true, 1, 0)) / sum(visitors)) * 100 <= 50 then "商品支付率低于百分之五十该启用预警"
           end      as product_force_warning
from dws_a
         LATERAL VIEW explode(array(1, 7, 30)) days_table AS days
WHERE to_date(concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))) >=
    date_add('2025-03-27', -days + 1)
group by shop_id, product_id, days;
