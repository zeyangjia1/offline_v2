for i in order_info  user_info  order_detail  sku_info  spu_info user_info sku_sale_attr_value sku_info sku_attr_value refund_payment payment_info order_status_log  order_refund_info order_info order_detail_coupon order_detail_activity order_detail favor_info coupon_use coupon_info comment_info cart_info base_trademark base_region base_province base_dic base_category3 base_category2 base_category1 activity_rule activity_info  
do
sqoop import --connect jdbc:mysql://cdh03:3306/offiline_v1 \
 --username root  \
 --password root \
 --delete-target-dir \
 -m 1 \
 --table $i \
 --target-dir /offiline_data/gmall/ods/$i/'2025-03-24' \
 -z \
 --null-string '\\N' \
 --null-non-string '\\N' \
 --fields-terminated-by '\t' 
done

