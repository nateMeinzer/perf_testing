select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 867 and 867+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blanched' or i_color = 'lemon') and 
        (i_units = 'Box' or i_units = 'Unknown') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'red' or i_color = 'moccasin') and
        (i_units = 'Dozen' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'chiffon') and
        (i_units = 'Ounce' or i_units = 'Bundle') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'linen' or i_color = 'firebrick') and
        (i_units = 'N/A' or i_units = 'Tbl') and
        (i_size = 'medium' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'cream' or i_color = 'papaya') and 
        (i_units = 'Oz' or i_units = 'Each') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'floral' or i_color = 'beige') and
        (i_units = 'Gross' or i_units = 'Gram') and
        (i_size = 'N/A' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'ghost' or i_color = 'pale') and
        (i_units = 'Lb' or i_units = 'Pallet') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'seashell' or i_color = 'wheat') and
        (i_units = 'Ton' or i_units = 'Bunch') and
        (i_size = 'medium' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
