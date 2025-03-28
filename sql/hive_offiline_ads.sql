show databases ;
use offilne;




drop table ads_Three_day_proportion;
create external table ads_Three_day_proportion(
                                                  user_zb string comment '占比',
                                                  Description string comment '描述'
)COMMENT '最近七日连续三天下单的用户数占比表'
    stored as textfile
    LOCATION '/warehouse/gmall/ads/ads_Three_day_proportion/'
    tblproperties ("textfile.Compression=gzip");

drop table ads_Brand_re_purchase_rate;
create external table ads_Brand_re_purchase_rate(
                                                    tm_name string comment '品牌',
                                                    days bigint comment '1,7,30天',
                                                    dt string comment '统计日期',
                                                    lv string comment '复购率'
)    COMMENT '最近1,7,30天复购率表'
    stored as textfile
    LOCATION '/warehouse/gmall/ads/ads_Brand_re_purchase_rate/'
    tblproperties ("textfile.Compression=gzip");



drop table ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source` STRING COMMENT '跳转起始页面ID',
    `target` STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
)   COMMENT '最近1,7,30天页面起始终止跳转次数表'
    stored as textfile
    LOCATION '/warehouse/gmall/ads/ads_page_path/'
    tblproperties ("textfile.Compression=gzip");
--1,7,30天复购率

WITH recent_purchases AS (
    -- 生成最近1天、7天、30天的记录
    SELECT
        tm_id,
        user_id,
        dt,
        recent_days
    FROM
        dwt_Grand_summary_1
            LATERAL VIEW
    explode(array(1, 7, 30)) days AS recent_days
WHERE
    dt>= date_add('2025-03-25',-recent_days+1)
    ),
    total_and_repeat_users AS (
-- 计算每个品牌在每个时间周期内的总用户数和复购用户数
SELECT
    tm_id,
    recent_days,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN purchase_count >= 2 THEN user_id END) AS repeat_users
FROM (
    SELECT
    tm_id,
    user_id,
    recent_days,
    COUNT(*) AS purchase_count
    FROM
    recent_purchases
    GROUP BY
    tm_id, user_id, recent_days
    ) subquery
GROUP BY
    tm_id, recent_days
    )
insert into table  ads_Brand_re_purchase_rate
SELECT
    tm_id,
    recent_days,
    "2025-03-25",
    -- 计算复购率
    concat(cast(ROUND(repeat_users / total_users * 100, 2) as string), '%') AS lv
FROM
    total_and_repeat_users;

select * from ads_Brand_re_purchase_rate;

--最近七日连续三天下单的用户数占比
insert into table ads_Three_day_proportion
select
    concat((c.user_nums/d.counts)*100,'%') as lv,
    c.miaoshu
from
    (select
         count(distinct b.user_id) as     user_nums,
         "最近七日连续三天下单占比" as miaoshu
     from
         (select
              a.dt as dt ,
              a.user_id as user_id ,
              lag(dt,2) over (partition by user_id order by dt) as days
          from (select distinct user_id, dt from dwt_Grand_summary_1)a where a.dt>=date_sub('2025-03-25',7))
             b where datediff(b.dt,b.days)=2
    ) c join
    (select count(*) as counts
     from dwt_Grand_summary_1 where dt>=date_sub('2025-03-25',7))d;



insert into  table  ads_page_path
select
    '2025-03-25' as dt,
    recent_days,
    concat(step,soure) as soure,
    concat(step+1,targent) as targent,
    count(*) as path_count
from (         select
                   *,
                   page_id as soure,
                   lead(page_id) over (partition by sid order by ts) as targent,
                       row_number() over (partition by sid order by ts) as step
               from (
                        select
                            *,
                            concat(mid_id,'-',last_value(`if`(last_page_id is null,ts,null),true)
                                over (partition by mid_id order by ts)) sid
                        from dwd_page_log lateral view explode (Array(1,7,30)) tmp as recent_days
                        where dt>=date_add('2025-03-25',-recent_days+1)
                    )t1
     ) t2
group by mid_id, sid, concat(step,soure), concat(step+1,targent),recent_days;








