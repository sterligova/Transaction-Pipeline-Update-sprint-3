-- alter f_sales add column
ALTER TABLE mart.f_sales 
ADD COLUMN status varchar(20);

-- alter staging.user_order_log add column
ALTER TABLE staging.user_order_log  
ADD COLUMN status varchar(20);

-- set default value table staging.user_order_log 
ALTER TABLE staging.user_order_log ALTER COLUMN status SET DEFAULT 'shipped';