use
offilne;
set
hive.exec.mode.local.auto=true;

--
drop function split_log;

create function split_log as 'log_split_and_Interceptor.split_log'
    using jar "hdfs://cdh01:8020/jar/offline_v1-1.0-SNAPSHOT.jar";

-------------------------dwd层建表---------------------
---------------日志-----------------
-- 启动日志表
DROP TABLE IF EXISTS dwd_start_log;
CREATE
EXTERNAL TABLE dwd_start_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `entry` STRING COMMENT 'icon手机图标 notice 通知 install 安装后启动',
    `loading_time` BIGINT COMMENT '启动加载时间',
    `open_ad_id` STRING COMMENT '广告页ID ',
    `open_ad_ms` BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点',
    `ts` BIGINT COMMENT '时间'
) COMMENT '启动日志表'
PARTITIONED BY (`dt` STRING) -- 按照时间创建分区
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_start_log/'
    tblproperties ("textfile.Compression=gzip");
describe dwd_start_log;
insert
overwrite  table dwd_start_log partition (dt="2025-03-23")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.ts')
from ods_log
where get_json_object(line, '$.start') is not null
  and dt = '2025-03-23';

insert
overwrite  table dwd_start_log partition (dt="2025-03-24")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.ts')
from ods_log
where get_json_object(line, '$.start') is not null
  and dt = '2025-03-24';
insert
overwrite  table dwd_start_log partition (dt="2025-03-25")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.ts')
from ods_log
where get_json_object(line, '$.start') is not null
  and dt = '2025-03-25';


-- 页面日志表
DROP TABLE IF EXISTS dwd_page_log;
CREATE
EXTERNAL TABLE dwd_page_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT '持续时间毫秒',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面ID ',
    `source_type` STRING COMMENT '来源类型',
    `ts` bigint
) COMMENT '页面日志表'
PARTITIONED BY (`dt` STRING)
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_page_log/'
    tblproperties ("textfile.Compression=gzip");
describe dwd_page_log;
insert
overwrite  table dwd_page_log partition (dt="2025-03-23")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts')
from ods_log
where dt = '2025-03-23'
  and get_json_object(line, '$.page') is not null;

insert
overwrite  table dwd_page_log partition (dt="2025-03-24")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts')
from ods_log
where dt = '2025-03-24'
  and get_json_object(line, '$.page') is not null;

insert
overwrite  table dwd_page_log partition (dt="2025-03-25")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts')
from ods_log
where dt = '2025-03-25'
  and get_json_object(line, '$.page') is not null;

-- 动作日志表
DROP TABLE IF EXISTS dwd_action_log;
CREATE
EXTERNAL TABLE dwd_action_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT '持续时间毫秒',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面id ',
    `source_type` STRING COMMENT '来源类型',
    `action_id` STRING COMMENT '动作id',
    `item` STRING COMMENT '目标id ',
    `item_type` STRING COMMENT '目标类型',
    `ts` BIGINT COMMENT '时间'
) COMMENT '动作日志表'
PARTITIONED BY (`dt` STRING)
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_action_log/'
    tblproperties ("textfile.Compression=gzip");




insert
overwrite  table dwd_action_log partition (dt="2025-03-23")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(action,'$.action_id'),
       get_json_object(action,'$.item'),
       get_json_object(action,'$.item_type'),
       get_json_object(action,'$.ts')
from ods_log line lateral view split_log (get_json_object(line,'$.actions')) tmp as action
where dt='2025-03-23'
  and get_json_object(line
    , '$.actions') is not null;

insert
overwrite  table dwd_action_log partition (dt="2025-03-24")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(action,'$.action_id'),
       get_json_object(action,'$.item'),
       get_json_object(action,'$.item_type'),
       get_json_object(action,'$.ts')
from ods_log line lateral view split_log (get_json_object(line,'$.actions')) tmp as action
where dt='2025-03-24'
  and get_json_object(line
    , '$.actions') is not null;
insert
overwrite  table dwd_action_log partition (dt="2025-03-25")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(action,'$.action_id'),
       get_json_object(action,'$.item'),
       get_json_object(action,'$.item_type'),
       get_json_object(action,'$.ts')
from ods_log line lateral view split_log (get_json_object(line,'$.actions')) tmp as action
where dt='2025-03-25'
  and get_json_object(line
    , '$.actions') is not null;

-- 曝光日志表
DROP TABLE IF EXISTS dwd_display_log;
CREATE
EXTERNAL TABLE dwd_display_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT 'app版本号',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面ID ',
    `source_type` STRING COMMENT '来源类型',
    `ts` BIGINT COMMENT 'app版本号',
    `display_type` STRING COMMENT '曝光类型',
    `item` STRING COMMENT '曝光对象id ',
    `item_type` STRING COMMENT 'app版本号',
    `order` BIGINT COMMENT '曝光顺序',
    `pos_id` BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
PARTITIONED BY (`dt` STRING)
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_display_log/'
    tblproperties ("textfile.Compression=gzip");



insert
overwrite  table dwd_display_log partition (dt="2025-03-23")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts'),
       get_json_object(display, '$.display_type'),
       get_json_object(display, '$.item'),
       get_json_object(display, '$.item_type'),
       get_json_object(display, '$.order'),
       get_json_object(display, '$.pos_id')
from ods_log line lateral view split_log(get_json_object(line,'$.displays')) tmp as display
where dt='2025-03-23'
  and get_json_object(line
    , '$.displays') is not null;


insert
overwrite  table dwd_display_log partition (dt="2025-03-24")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts'),
       get_json_object(display, '$.display_type'),
       get_json_object(display, '$.item'),
       get_json_object(display, '$.item_type'),
       get_json_object(display, '$.order'),
       get_json_object(display, '$.pos_id')
from ods_log line lateral view split_log(get_json_object(line,'$.displays')) tmp as display
where dt='2025-03-24'
  and get_json_object(line
    , '$.displays') is not null;
insert
overwrite  table dwd_display_log partition (dt="2025-03-25")
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts'),
       get_json_object(display, '$.display_type'),
       get_json_object(display, '$.item'),
       get_json_object(display, '$.item_type'),
       get_json_object(display, '$.order'),
       get_json_object(display, '$.pos_id')
from ods_log line lateral view split_log(get_json_object(line,'$.displays')) tmp as display
where dt='2025-03-25'
  and get_json_object(line
    , '$.displays') is not null;


-- 错误日志表
DROP TABLE IF EXISTS dwd_error_log;
CREATE
EXTERNAL TABLE dwd_error_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面ID ',
    `source_type` STRING COMMENT '来源类型',
    `entry` STRING COMMENT ' icon手机图标  notice 通知 install 安装后启动',
    `loading_time` STRING COMMENT '启动加载时间',
    `open_ad_id` STRING COMMENT '广告页ID ',
    `open_ad_ms` STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `actions` STRING COMMENT '动作',
    `displays` STRING COMMENT '曝光',
    `ts` STRING COMMENT '时间',
    `error_code` STRING COMMENT '错误码',
    `msg` STRING COMMENT '错误信息'
) COMMENT '错误日志表'
PARTITIONED BY (`dt` STRING)
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_error_log/'
    tblproperties ("textfile.Compression=gzip");


describe dwd_error_log;

insert
overwrite table dwd_error_log partition(dt='2025-03-23')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.actions'),
       get_json_object(line, '$.displays'),
       get_json_object(line, '$.ts'),
       get_json_object(line, '$.err.error_code'),
       get_json_object(line, '$.err.msg')
from ods_log
where dt = '2025-03-23'
  and get_json_object(line, '$.err') is not null;
insert
overwrite table dwd_error_log partition(dt='2025-03-24')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.actions'),
       get_json_object(line, '$.displays'),
       get_json_object(line, '$.ts'),
       get_json_object(line, '$.err.error_code'),
       get_json_object(line, '$.err.msg')
from ods_log
where dt = '2025-03-24'
  and get_json_object(line, '$.err') is not null;
insert
overwrite table dwd_error_log partition(dt='2025-03-25')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.actions'),
       get_json_object(line, '$.displays'),
       get_json_object(line, '$.ts'),
       get_json_object(line, '$.err.error_code'),
       get_json_object(line, '$.err.msg')
from ods_log
where dt = '2025-03-25'
  and get_json_object(line, '$.err') is not null;

select *
from dwd_error_log;



DROP TABLE IF EXISTS dwd_comment_info;
CREATE
EXTERNAL TABLE dwd_comment_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品sku',
    `order_id` STRING COMMENT '订单ID',
    `appraise` STRING COMMENT '评价(好评、中评、差评、默认评价)',
    `create_time` STRING COMMENT '评价时间'
) COMMENT '评价事实表'
PARTITIONED BY (`dt` STRING)
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_comment_info/'
    tblproperties ("textfile.Compression=gzip");

describe dwd_comment_info;



select *
from dwd_comment_info dci
         join dwd_order_info doi
              on dci.order_id = doi.id;

insert
overwrite table  dwd_comment_info partition (dt='2025-03-23')
select `id`,
       `user_id`,
       `sku_id`,
       `order_id`,
       `appraise`,
       `create_time`
from ods_comment_info
where dt = '2025-03-23';
insert
overwrite table  dwd_comment_info partition (dt='2025-03-24')
select `id`,
       `user_id`,
       `sku_id`,
       `order_id`,
       `appraise`,
       `create_time`
from ods_comment_info
where dt = '2025-03-24';
insert
overwrite table  dwd_comment_info partition (dt='2025-03-25')
select `id`,
       `user_id`,
       `sku_id`,
       `order_id`,
       `appraise`,
       `create_time`
from ods_comment_info
where dt = '2025-03-25';



drop table dwd_order_detail;
CREATE
EXTERNAL TABLE dwd_order_detail (
    `id` STRING COMMENT '订单编号',
    `order_id` STRING COMMENT '订单号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'sku商品id',
    `province_id` STRING COMMENT '省份ID',
    `create_time` STRING COMMENT '创建时间',
    `sku_num` BIGINT COMMENT '商品数量'
) COMMENT '订单明细事实表表'
PARTITIONED BY (`dt` STRING)
    stored as textfile
    LOCATION '/warehouse/gmall/dwd/dwd_order_detail/'
    tblproperties ("textfile.Compression=gzip");
describe dwd_order_detail;



insert
overwrite table dwd_order_detail partition (dt='2025-03-23')
select ood.id,
       ooi.id,
       ooi.user_id,
       ood.sku_id,
       ooi.province_id,
       ooi.create_time,
       ood.sku_num
from ods_order_info ooi
         join ods_order_detail ood
              on ooi.id = ood.order_id
where ooi.dt = '2025-03-23';


insert
overwrite table dwd_order_detail partition (dt='2025-03-24')
select ood.id,
       ooi.id,
       ooi.user_id,
       ood.sku_id,
       ooi.province_id,
       ooi.create_time,
       ood.sku_num
from ods_order_info ooi
         join ods_order_detail ood
              on ooi.id = ood.order_id
where ooi.dt = '2025-03-24';
insert
overwrite table dwd_order_detail partition (dt='2025-03-25')
select ood.id,
       ooi.id,
       ooi.user_id,
       ood.sku_id,
       ooi.province_id,
       ooi.create_time,
       ood.sku_num
from ods_order_info ooi
         join ods_order_detail ood
              on ooi.id = ood.order_id
where ooi.dt = '2025-03-25';



drop table dwd_order_info;
CREATE
EXTERNAL TABLE dwd_order_info (
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
    LOCATION '/warehouse/gmall/dwd/dwd_order_info/'
    tblproperties ("textfile.Compression=gzip");


insert
overwrite table dwd_order_info partition (dt='2025-03-23')
select `id`,
       `final_amount`,
       `order_status`,
       `user_id`,
       `payment_way`,
       `delivery_address`,
       `out_trade_no`,
       `create_time`,
       `operate_time`,
       `expire_time`,
       `tracking_no`,
       `province_id`,
       `activity_reduce_amount`,
       `coupon_reduce_amount`,
       `original_amount`,
       `feight_fee`,
       `feight_fee_reduce`
from ods_order_info
where dt = '2025-03-23';
insert
overwrite table dwd_order_info partition (dt='2025-03-24')
select `id`,
       `final_amount`,
       `order_status`,
       `user_id`,
       `payment_way`,
       `delivery_address`,
       `out_trade_no`,
       `create_time`,
       `operate_time`,
       `expire_time`,
       `tracking_no`,
       `province_id`,
       `activity_reduce_amount`,
       `coupon_reduce_amount`,
       `original_amount`,
       `feight_fee`,
       `feight_fee_reduce`
from ods_order_info
where dt = '2025-03-24';
insert
overwrite table dwd_order_info partition (dt='2025-03-25')
select `id`,
       `final_amount`,
       `order_status`,
       `user_id`,
       `payment_way`,
       `delivery_address`,
       `out_trade_no`,
       `create_time`,
       `operate_time`,
       `expire_time`,
       `tracking_no`,
       `province_id`,
       `activity_reduce_amount`,
       `coupon_reduce_amount`,
       `original_amount`,
       `feight_fee`,
       `feight_fee_reduce`
from ods_order_info
where dt = '2025-03-25';



drop table dwd_sku_info;
CREATE
EXTERNAL TABLE dwd_sku_info(
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
    LOCATION '/warehouse/gmall/dwd/dwd_sku_info/'
    tblproperties ("textfile.Compression=gzip")
;
insert
overwrite table dwd_sku_info partition (dt='2025-03-23')
select `id`,
       `spu_id`,
       `price`,
       `sku_name`,
       `sku_desc`,
       `weight`,
       `tm_id`,
       `category3_id`,
       `is_sale`,
       `create_time`
from ods_sku_info
where dt = '2025-03-23';
insert
overwrite table dwd_sku_info partition (dt='2025-03-24')
select `id`,
       `spu_id`,
       `price`,
       `sku_name`,
       `sku_desc`,
       `weight`,
       `tm_id`,
       `category3_id`,
       `is_sale`,
       `create_time`
from ods_sku_info
where dt = '2025-03-24';
describe dwd_page_log;

insert
overwrite table dwd_sku_info partition (dt='2025-03-25')
select `id`,
       `spu_id`,
       `price`,
       `sku_name`,
       `sku_desc`,
       `weight`,
       `tm_id`,
       `category3_id`,
       `is_sale`,
       `create_time`
from ods_sku_info
where dt = '2025-03-25';


drop table dwd_user_info;
CREATE
EXTERNAL TABLE dwd_user_info(
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
    LOCATION '/warehouse/gmall/dwd/dwd_user_info/'
    tblproperties ("textfile.Compression=gzip");
describe dwd_user_info;

insert
overwrite table dwd_user_info partition (dt='2025-03-23')
select `id`,
       `login_name`,
       `nick_name`,
       `name`,
       `phone_num`,
       `email`,
       `user_level`,
       `birthday`,
       `gender`,
       `create_time`,
       `operate_time`
from ods_user_info
where dt = '2025-03-23';
insert
overwrite table dwd_user_info partition (dt='2025-03-24')
select `id`,
       `login_name`,
       `nick_name`,
       `name`,
       `phone_num`,
       `email`,
       `user_level`,
       `birthday`,
       `gender`,
       `create_time`,
       `operate_time`
from ods_user_info
where dt = '2025-03-24';
insert
overwrite table dwd_user_info partition (dt='2025-03-25')
select `id`,
       `login_name`,
       `nick_name`,
       `name`,
       `phone_num`,
       `email`,
       `user_level`,
       `birthday`,
       `gender`,
       `create_time`,
       `operate_time`
from ods_user_info
where dt = '2025-03-25';
describe ods_user_info;
