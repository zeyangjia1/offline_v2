use  offilne;
set hive.exec.mode.local.auto=true;

drop table dwt_Grand_summary_1;
create external table  dwt_Grand_summary_1(
    user_id string,
    sku_id string,
    tm_id string,
    sku_num bigint,
    dt string,
    appraise bigint,
    order_status bigint,
    province_id bigint
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as textfile
    LOCATION '/warehouse/gmall/dwt/dwt_Grand_summary_1/'
    tblproperties ("textfile.Compression=gzip");
describe dwt_Grand_summary_1;
SELECT * from dwt_Grand_summary_1;

insert into table dwt_Grand_summary_1
select
    ooi.user_id,
    ood.sku_id,
    odi.tm_id,
    ood.sku_num,
    ooi.dt,
    oci.appraise,
    ooi.order_status,
    ooi.province_id
from ods_order_info ooi
         join ods_order_detail ood  on ooi.id=ood.order_id
         join ods_sku_info odi on ood.sku_id=odi.id
         join ods_comment_info oci on oci.order_id=ooi.id;

drop table dwt_page_log;
CREATE EXTERNAL TABLE dwt_page_log(
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
                                      `ts` bigint,
                                     dt string
) COMMENT '页面日志表'
    stored as textfile
    LOCATION '/warehouse/gmall/dwt/dwt_page_log/'
    tblproperties ("textfile.Compression=gzip");
insert into table dwt_page_log
select * from dwd_page_log;