select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 26 and 26 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-06-09' as date) and DATE_ADD(cast('2001-06-09' as date), 60)
 and i_manufact_id in (744,884,722,693)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;
