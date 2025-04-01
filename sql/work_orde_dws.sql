use worker_oder;
set hive.exec.mode.local.auto=true;

create table worker_oder.dws_A
(
    price        DECIMAL(10, 2) COMMENT '商品价格',
    stock        INT COMMENT '商品库存',
    order_id     STRING COMMENT '订单 ID',
    product_id   STRING COMMENT '商品 ID',
    shop_id      STRING COMMENT '店铺 ID',
    order_date   DATE COMMENT '订单日期',
    quantity     INT COMMENT '订单数量',
    amount       DECIMAL(10, 2) COMMENT '订单金额',
    is_paid      BOOLEAN COMMENT '是否支付',
    traffic_id   STRING COMMENT '流量 ID',
    traffic_date DATE COMMENT '流量日期',
    source       STRING COMMENT '流量来源',
    visitors     INT COMMENT '访客数',
    paid_buyers  INT COMMENT '支付买家数',
    search_id    STRING COMMENT '搜索 ID',
    search_date  DATE COMMENT '搜索日期',
    keyword      STRING COMMENT '搜索关键词'
)
    partitioned by (dt string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
insert overwrite table dws_A partition (dt = 20250327)
select price,
       stock,
       order_id,
       oo.product_id,
       oo.shop_id,
       order_date,
       quantity,
       amount,
       is_paid,
       traffic_id,
       traffic_date,
       source,
       visitors,
       paid_buyers,
       search_id,
       search_date,
       keyword
from ods_order oo
         inner join ods_search os on os.shop_id = oo.shop_id
         inner join ods_product op on oo.product_id = op.product_id
         inner join ods_traffic ot on oo.product_id = ot.product_id
where oo.dt = "20250327"
;