-- 1. 商品销售事实表（按销售日期分区）
use worker_oder;

-- 创建 ODS 层商品表
CREATE TABLE worker_oder.ods_product
(
    product_id STRING COMMENT '商品 ID',
    shop_id    STRING COMMENT '店铺 ID',
    price      DECIMAL(10, 2) COMMENT '商品价格',
    stock      INT COMMENT '商品库存'
) partitioned by (dt string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");

load data inpath "/work_orde/ods/product/20250325" into table ods_product partition (dt = "20250325");
load data inpath "/work_orde/ods/product/20250326" into table ods_product partition (dt = "20250326");
load data inpath "/work_orde/ods/product/20250327" into table ods_product partition (dt = "20250327");

select *
from ods_product;

-- 创建 ODS 层订单表
CREATE TABLE worker_oder.ods_order
(
    order_id   STRING COMMENT '订单 ID',
    product_id STRING COMMENT '商品 ID',
    shop_id    STRING COMMENT '店铺 ID',
    order_date DATE COMMENT '订单日期',
    quantity   INT COMMENT '订单数量',
    amount     DECIMAL(10, 2) COMMENT '订单金额',
    is_paid    BOOLEAN COMMENT '是否支付'
) partitioned by (dt string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
load data inpath "/work_orde/ods/order/20250325" into table ods_order partition (dt = "20250325");
load data inpath "/work_orde/ods/order/20250326" into table ods_order partition (dt = "20250326");
load data inpath "/work_orde/ods/order/20250327" into table ods_order partition (dt = "20250327");
-- 创建 ODS 层流量表
CREATE TABLE worker_oder.ods_traffic
(
    traffic_id   STRING COMMENT '流量 ID',
    product_id   STRING COMMENT '商品 ID',
    shop_id      STRING COMMENT '店铺 ID',
    traffic_date DATE COMMENT '流量日期',
    source       STRING COMMENT '流量来源',
    visitors     INT COMMENT '访客数',
    paid_buyers  INT COMMENT '支付买家数'
) partitioned by (dt string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
load data inpath "/work_orde/ods/traffic/20250325" into table ods_traffic partition (dt = "20250325");
load data inpath "/work_orde/ods/traffic/20250326" into table ods_traffic partition (dt = "20250326");
load data inpath "/work_orde/ods/traffic/20250327" into table ods_traffic partition (dt = "20250327");
select *
from ods_traffic;
-- 创建 ODS 层搜索表
CREATE TABLE worker_oder.ods_search
(
    search_id   STRING COMMENT '搜索 ID',
    product_id  STRING COMMENT '商品 ID',
    shop_id     STRING COMMENT '店铺 ID',
    search_date DATE COMMENT '搜索日期',
    keyword     STRING COMMENT '搜索关键词'

)
    partitioned by (dt string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS orc
    tblproperties ("orc.Compression=Snappy");
load data inpath "/work_orde/ods/search/20250325" into table ods_search partition (dt = "20250325");
load data inpath "/work_orde/ods/search/20250326" into table ods_search partition (dt = "20250326");
load data inpath "/work_orde/ods/search/20250327" into table ods_search partition (dt = "20250327");