---d_calendar
  
truncate table mart.d_calendar cascade;
truncate table mart.d_customer cascade;
truncate table mart.d_item cascade;

CREATE SEQUENCE d_calendar_date_id start 1; 
insert into mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
with all_dates as (
select date_id as date_time
from stage.customer_research cr 
UNION
select date_time
from stage.user_activity_log ual
UNION
select date_time
from stage.user_order_log uol
)
select 
  nextval('d_calendar_date_id') as date_id
, date_time as fact_date
, extract(DAY from date_time)::INTEGER as day_num
, extract(MONTH from date_time)::INTEGER as month_num
, TO_CHAR(date_time, 'Month') as month_name
, extract(YEAR from date_time)::INTEGER as year_num
from all_dates
order by date_time ASC;
DROP SEQUENCE d_calendar_date_id;
   

---d_customer
insert into mart.d_customer (customer_id, city_id, first_name, last_name)
select
  customer_id
, city_id
, first_name
, last_name
from (
select distinct
  customer_id
, city_id
, first_name
, last_name
, row_number() over(partition by customer_id order by city_id DESC) as rn
from stage.user_order_log uol) as u
where rn = 1;
   
---d_item
insert into mart.d_item (item_id, item_name)
select DISTINCT
  item_id
, item_name
from stage.user_order_log;