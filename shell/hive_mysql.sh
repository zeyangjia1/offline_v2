#!/bin/bash

# MySQL 连接信息
MYSQL_HOST="cdh03"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="root"
MYSQL_DATABASE="offiline_v1"
MYSQL_TABLE="ads_Sales_volume"

# Hive 表信息
HIVE_TABLE="worker_oder.ads_Sales_volume"

# Sqoop 导出命令
sqoop export \
  --connect "jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DATABASE" \
  --username $MYSQL_USER \
  --password $MYSQL_PASSWORD \
  --table $MYSQL_TABLE \
  --export-dir "/home/user/hive/warehouse/worker_oder.db/ads_sales_volume" \
  --input-fields-terminated-by '\,' \
  --input-lines-terminated-by '\n'    

