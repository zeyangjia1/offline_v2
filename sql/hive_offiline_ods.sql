use  offilne;
set hive.exec.mode.local.auto=true;

-------------------------ods层建表-----------------------


show functions ;
----------------日志-----------------
drop table if exists ods_log;
CREATE EXTERNAL TABLE ods_log (`line` string)

PARTITIONED BY (`dt` string) -- 按照时间创建分区

stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_log'
tblproperties ("textfile.Compression=gzip");

  -- 指定数据在hdfs上的存储位置;
load data inpath "/offiline_data/gmall/ods/logs/2025-03-23" into table ods_log partition (dt="2025-03-23");
load data inpath "/offiline_data/gmall/ods/logs/2025-03-24" into table ods_log partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/logs/2025-03-25" into table ods_log partition (dt="2025-03-25");
select * from ods_log;
----------------业务-----------------
-- 1 活动信息表
DROP TABLE IF EXISTS ods_activity_info;
CREATE EXTERNAL TABLE ods_activity_info(
    `id` STRING COMMENT '编号',
    `activity_name` STRING  COMMENT '活动名称',
    `activity_type` STRING  COMMENT '活动类型',
    `start_time` STRING  COMMENT '开始时间',
    `end_time` STRING  COMMENT '结束时间',
    `create_time` STRING  COMMENT '创建时间'
) COMMENT '活动信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_activity_info/'
    tblproperties ("textfile.Compression=gzip");

load data inpath "/offiline_data/gmall/ods/activity_info/2025-03-25" into table ods_activity_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/activity_info/2025-03-24" into table ods_activity_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/activity_info/2025-03-23" into table ods_activity_info partition (dt="2025-03-23");

DROP TABLE IF EXISTS ods_activity_rule;
CREATE EXTERNAL TABLE ods_activity_rule(
    `id` STRING COMMENT '编号',
    `activity_id` STRING  COMMENT '活动ID',
    `activity_type` STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num` BIGINT COMMENT '满减件数',
    `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
    `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_activity_rule/'
    tblproperties ("textfile.Compression=gzip")
;
load data inpath "/offiline_data/gmall/ods/activity_rule/2025-03-25" into table ods_activity_rule partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/activity_rule/2025-03-24" into table ods_activity_rule partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/activity_rule/2025-03-23" into table ods_activity_rule partition (dt="2025-03-23");

-- 3 一级品类表
DROP TABLE IF EXISTS ods_base_category1;
CREATE EXTERNAL TABLE ods_base_category1(
    `id` STRING COMMENT 'id',
    `name` STRING COMMENT '名称'
) COMMENT '商品一级分类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_category1/'
    tblproperties ("textfile.Compression=gzip")
;



load data inpath "/offiline_data/gmall/ods/base_category1/2025-03-25" into table ods_base_category1 partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_category1/2025-03-24" into table ods_base_category1 partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_category1/2025-03-23" into table ods_base_category1 partition (dt="2025-03-23");

-- 4 二级品类表
DROP TABLE IF EXISTS ods_base_category2;
CREATE EXTERNAL TABLE ods_base_category2(
    `id` STRING COMMENT ' id',
    `name` STRING COMMENT '名称',
    `category1_id` STRING COMMENT '一级品类id'
) COMMENT '商品二级分类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_category2/'
    tblproperties ("textfile.Compression=gzip")
;


load data inpath "/offiline_data/gmall/ods/base_category2/2025-03-25" into table ods_base_category2 partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_category2/2025-03-24" into table ods_base_category2 partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_category2/2025-03-23" into table ods_base_category2 partition (dt="2025-03-23");

-- 5 三级品类表
DROP TABLE IF EXISTS ods_base_category3;
CREATE EXTERNAL TABLE ods_base_category3(
    `id` STRING COMMENT ' id',
    `name` STRING COMMENT '名称',
    `category2_id` STRING COMMENT '二级品类id'
) COMMENT '商品三级分类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_category3/'
    tblproperties ("textfile.Compression=gzip")
;


load data inpath "/offiline_data/gmall/ods/base_category3/2025-03-25" into table ods_base_category3 partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_category3/2025-03-24" into table ods_base_category3 partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_category3/2025-03-23" into table ods_base_category3 partition (dt="2025-03-23");

-- 6 编码字典表
DROP TABLE IF EXISTS ods_base_dic;
CREATE EXTERNAL TABLE ods_base_dic(
    `dic_code` STRING COMMENT '编号',
    `dic_name` STRING COMMENT '编码名称',
    `parent_code` STRING COMMENT '父编码',
    `create_time` STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '操作日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_dic/'
    tblproperties ("textfile.Compression=gzip")
;


load data inpath "/offiline_data/gmall/ods/base_dic/2025-03-25" into table ods_base_dic partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_dic/2025-03-24" into table ods_base_dic partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_dic/2025-03-23" into table ods_base_dic partition (dt="2025-03-23");

-- 7 省份表
DROP TABLE IF EXISTS ods_base_province;
CREATE EXTERNAL TABLE ods_base_province (
    `id` STRING COMMENT '编号',
    `name` STRING COMMENT '省份名称',
    `region_id` STRING COMMENT '地区ID',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用'
)  COMMENT '省份表'
    partitioned by (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_province/'
    tblproperties ("textfile.Compression=gzip")
;


load data inpath "/offiline_data/gmall/ods/base_province/2025-03-25" into table ods_base_province partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_province/2025-03-24" into table ods_base_province partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_province/2025-03-23" into table ods_base_province partition (dt="2025-03-23");

-- 8 地区表
DROP TABLE IF EXISTS ods_base_region;
CREATE EXTERNAL TABLE ods_base_region (
    `id` STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
)  COMMENT '地区表'
    partitioned by (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_region/'
    tblproperties ("textfile.Compression=gzip")
;

load data inpath "/offiline_data/gmall/ods/base_region/2025-03-25" into table ods_base_region partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_region/2025-03-24" into table ods_base_region partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_region/2025-03-23" into table ods_base_region partition (dt="2025-03-23");

-- 9 品牌表
DROP TABLE IF EXISTS ods_base_trademark;
CREATE EXTERNAL TABLE ods_base_trademark (
    `id` STRING COMMENT '编号',
    `tm_name` STRING COMMENT '品牌名称'
)  COMMENT '品牌表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_trademark/'
    tblproperties ("textfile.Compression=gzip")
;
load data inpath "/offiline_data/gmall/ods/base_trademark/2025-03-25" into table ods_base_trademark partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/base_trademark/2025-03-24" into table ods_base_trademark partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/base_trademark/2025-03-23" into table ods_base_trademark partition (dt="2025-03-23");

-- 10 购物车表
DROP TABLE IF EXISTS ods_cart_info;
CREATE EXTERNAL TABLE ods_cart_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'skuid',
    `cart_price` DECIMAL(16,2)  COMMENT '放入购物车时价格',
    `sku_num` BIGINT COMMENT '数量',
    `sku_name` STRING COMMENT 'sku名称 (冗余)',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered` STRING COMMENT '是否已经下单',
    `order_time` STRING COMMENT '下单时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号'
) COMMENT '加购表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_cart_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/cart_info/2025-03-25" into table ods_cart_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/cart_info/2025-03-24" into table ods_cart_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/cart_info/2025-03-23" into table ods_cart_info partition (dt="2025-03-23");

-- 11 评论表
DROP TABLE IF EXISTS ods_comment_info;
CREATE EXTERNAL TABLE ods_comment_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品sku',
    `spu_id` STRING COMMENT '商品spu',
    `order_id` STRING COMMENT '订单ID',
    `appraise` STRING COMMENT '评价',
    `create_time` STRING COMMENT '评价时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_comment_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/comment_info/2025-03-25" into table ods_comment_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/comment_info/2025-03-24" into table ods_comment_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/comment_info/2025-03-23" into table ods_comment_info partition (dt="2025-03-23");
select * from ods_comment_info;



-- 12 优惠券信息表
DROP TABLE IF EXISTS ods_coupon_info;
CREATE EXTERNAL TABLE ods_coupon_info(
    `id` STRING COMMENT '购物券编号',
    `coupon_name` STRING COMMENT '购物券名称',
    `coupon_type` STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16,2) COMMENT '满额数',
    `condition_num` BIGINT COMMENT '满件数',
    `activity_id` STRING COMMENT '活动编号',
    `benefit_amount` DECIMAL(16,2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '折扣',
    `create_time` STRING COMMENT '创建时间',
    `range_type` STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num` BIGINT COMMENT '最多领用次数',
    `taken_count` BIGINT COMMENT '已领用次数',
    `start_time` STRING COMMENT '开始领取时间',
    `end_time` STRING COMMENT '结束领取时间',
    `operate_time` STRING COMMENT '修改时间',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_coupon_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/coupon_info/2025-03-25" into table ods_coupon_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/coupon_info/2025-03-24" into table ods_coupon_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/coupon_info/2025-03-23" into table ods_coupon_info partition (dt="2025-03-23");

-- 13 优惠券领用表
DROP TABLE IF EXISTS ods_coupon_use;
CREATE EXTERNAL TABLE ods_coupon_use(
    `id` STRING COMMENT '编号',
    `coupon_id` STRING  COMMENT '优惠券ID',
    `user_id` STRING  COMMENT 'skuid',
    `order_id` STRING  COMMENT 'spuid',
    `coupon_status` STRING  COMMENT '优惠券状态',
    `get_time` STRING  COMMENT '领取时间',
    `using_time` STRING  COMMENT '使用时间(下单)',
    `used_time` STRING  COMMENT '使用时间(支付)',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_coupon_use/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/coupon_use/2025-03-25" into table ods_coupon_use partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/coupon_use/2025-03-24" into table ods_coupon_use partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/coupon_use/2025-03-23" into table ods_coupon_use partition (dt="2025-03-23");

-- 14 收藏表
DROP TABLE IF EXISTS ods_favor_info;
CREATE EXTERNAL TABLE ods_favor_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'skuid',
    `spu_id` STRING COMMENT 'spuid',
    `is_cancel` STRING COMMENT '是否取消',
    `create_time` STRING COMMENT '收藏时间',
    `cancel_time` STRING COMMENT '取消时间'
) COMMENT '商品收藏表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_favor_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/favor_info/2025-03-25" into table ods_favor_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/favor_info/2025-03-24" into table ods_favor_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/favor_info/2025-03-23" into table ods_favor_info partition (dt="2025-03-23");

-- 15 订单明细表
DROP TABLE IF EXISTS ods_order_detail;
CREATE EXTERNAL TABLE ods_order_detail(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `sku_id` STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `order_price` DECIMAL(16,2) COMMENT '商品价格',
    `sku_num` BIGINT COMMENT '商品数量',
    `create_time` STRING COMMENT '创建时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `split_final_amount` DECIMAL(16,2) COMMENT '分摊最终金额',
    `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动优惠',
    `split_coupon_amount` DECIMAL(16,2) COMMENT '分摊优惠券优惠'
) COMMENT '订单详情表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_order_detail/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/order_detail/2025-03-25" into table ods_order_detail partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/order_detail/2025-03-24" into table ods_order_detail partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/order_detail/2025-03-23" into table ods_order_detail partition (dt="2025-03-23");

-- 16 订单表
DROP TABLE IF EXISTS ods_order_info;
CREATE EXTERNAL TABLE ods_order_info (
    `id` STRING COMMENT '订单号',
    `final_amount` DECIMAL(16,2) COMMENT '订单最终金额',
    `order_status` STRING COMMENT '订单状态',
    `user_id` STRING COMMENT '用户id',
    `payment_way` STRING COMMENT '支付方式',
    `delivery_address` STRING COMMENT '送货地址',
    `out_trade_no` STRING COMMENT '支付流水号',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `expire_time` STRING COMMENT '过期时间',
    `tracking_no` STRING COMMENT '物流单编号',
    `province_id` STRING COMMENT '省份ID',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免金额',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免金额',
    `original_amount` DECIMAL(16,2)  COMMENT '订单原价金额',
    `feight_fee` DECIMAL(16,2)  COMMENT '运费',
    `feight_fee_reduce` DECIMAL(16,2)  COMMENT '运费减免'
) COMMENT '订单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_order_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/order_info/2025-03-25" into table ods_order_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/order_info/2025-03-24" into table ods_order_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/order_info/2025-03-23" into table ods_order_info partition (dt="2025-03-23");

-- 17 退单表
DROP TABLE IF EXISTS ods_order_refund_info;
CREATE EXTERNAL TABLE ods_order_refund_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `refund_type` STRING COMMENT '退单类型',
    `refund_num` BIGINT COMMENT '退单件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `refund_status` STRING COMMENT '退单状态',--退单状态应包含买家申请、卖家审核、卖家收货、退款完成等状态。此处未涉及到，故该表按增量处理
    `create_time` STRING COMMENT '退单时间'
) COMMENT '退单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_order_refund_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/order_refund_info/2025-03-25" into table ods_order_refund_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/order_refund_info/2025-03-24" into table ods_order_refund_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/order_refund_info/2025-03-23" into table ods_order_refund_info partition (dt="2025-03-23");

--18 订单状态日志表
DROP TABLE IF EXISTS ods_order_status_log;
CREATE EXTERNAL TABLE ods_order_status_log (
    `id` STRING COMMENT '编号',
    `order_id` STRING COMMENT '订单ID',
    `order_status` STRING COMMENT '订单状态',
    `operate_time` STRING COMMENT '修改时间'
)  COMMENT '订单状态表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_order_status_log/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/order_status_log/2025-03-25" into table ods_order_status_log partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/order_status_log/2025-03-24" into table ods_order_status_log partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/order_status_log/2025-03-23" into table ods_order_status_log partition (dt="2025-03-23");

-- 19 支付表
DROP TABLE IF EXISTS ods_payment_info;
CREATE EXTERNAL TABLE ods_payment_info(
    `id` STRING COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` STRING COMMENT '订单编号',
    `user_id` STRING COMMENT '用户编号',
    `payment_type` STRING COMMENT '支付类型',
    `trade_no` STRING COMMENT '交易编号',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `subject` STRING COMMENT '交易内容',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间'
)  COMMENT '支付流水表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_payment_info/'
    tblproperties ("textfile.Compression=gzip");
load data inpath "/offiline_data/gmall/ods/payment_info/2025-03-25" into table ods_payment_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/payment_info/2025-03-24" into table ods_payment_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/payment_info/2025-03-23" into table ods_payment_info partition (dt="2025-03-23");


-- 20 商品（SKU）表
DROP TABLE IF EXISTS ods_sku_info;
CREATE EXTERNAL TABLE ods_sku_info(
    `id` STRING COMMENT 'skuId',
    `spu_id` STRING COMMENT 'spuid',
    `price` DECIMAL(16,2) COMMENT '价格',
    `sku_name` STRING COMMENT '商品名称',
    `sku_desc` STRING COMMENT '商品描述',
    `weight` DECIMAL(16,2) COMMENT '重量',
    `tm_id` STRING COMMENT '品牌id',
    `category3_id` STRING COMMENT '品类id',
    `is_sale` STRING COMMENT '是否在售',
    `create_time` STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_sku_info/'
    tblproperties ("textfile.Compression=gzip")
;



load data inpath "/offiline_data/gmall/ods/sku_info/2025-03-25" into table ods_sku_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/sku_info/2025-03-24" into table ods_sku_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/sku_info/2025-03-23" into table ods_sku_info partition (dt="2025-03-23");




-- 21 商品（SPU）表
DROP TABLE IF EXISTS ods_spu_info;
CREATE EXTERNAL TABLE ods_spu_info(
    `id` STRING COMMENT 'spuid',
    `spu_name` STRING COMMENT 'spu名称',
    `category3_id` STRING COMMENT '品类id',
    `tm_id` STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_spu_info/'
    tblproperties ("textfile.Compression=gzip")
;



load data inpath "/offiline_data/gmall/ods/spu_info/2025-03-25" into table ods_spu_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/spu_info/2025-03-24" into table ods_spu_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/spu_info/2025-03-23" into table ods_spu_info partition (dt="2025-03-23");

-- 22 用户表
DROP TABLE IF EXISTS ods_user_info;
CREATE EXTERNAL TABLE ods_user_info(
    `id` STRING COMMENT '用户id',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号码',
    `email` STRING COMMENT '邮箱',
    `user_level` STRING COMMENT '用户等级',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间'
) COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/ods/ods_user_info/'
    tblproperties ("textfile.Compression=gzip")
;




load data inpath "/offiline_data/gmall/ods/user_info/2025-03-25" into table ods_user_info partition (dt="2025-03-25");
load data inpath "/offiline_data/gmall/ods/user_info/2025-03-24" into table ods_user_info partition (dt="2025-03-24");
load data inpath "/offiline_data/gmall/ods/user_info/2025-03-23" into table ods_user_info partition (dt="2025-03-23");
