use  offilne;
set hive.exec.mode.local.auto=true;
set hive.support.concurrency=false;
set hive.auto.convert.join= false;
show databases ;

--最近七日连续三天下单的用户数占比
select
    a.miaoshu as miaoshu,
    (a.user_nums/aa.w) as user_zb
from
    (select
         count(distinct user_id) as user_nums,
         "最近七日连续三天下单占比" as miaoshu
     from
         (select
              dt,
              user_id,
              lag(dt,2) over (partition by user_id order by dt) as days
          from (select distinct user_id, dt from dwt_Grand_summary_1)
          where dt>=date_sub('2025-03-35',7)) a where datediff(dt,days)=2) a
        join
    (select count(*) as w
     from dwt_Grand_summary_1 where dt>=date_sub('2025-03-25',7))aa;


select
    dt,
    user_id,
    lag(dt,2) over (partition by user_id order by dt) as days
from (select distinct user_id, dt from dwt_Grand_summary_1)