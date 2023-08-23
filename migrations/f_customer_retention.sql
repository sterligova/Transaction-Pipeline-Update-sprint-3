DELETE FROM mart.f_customer_retention;
insert into mart.f_customer_retention (new_customers_count, returning_customers_count,
    refunded_customer_count, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
--  tables for calculations
WITH CTE AS
  (SELECT fs.item_id,
          fs.customer_id,
          fs.payment_amount,
          fs.status,
          dc.week_of_year AS period_id
   FROM mart.f_sales fs
   LEFT JOIN mart.d_calendar dc ON fs.date_id = dc.date_id),

CTE1 AS
  (SELECT period_id,
          customer_id,
          item_id,
          count(customer_id) AS num_orders,
          SUM (payment_amount) AS sum_payment_amount
   FROM CTE
   GROUP BY period_id,
            customer_id,
            item_id
   ORDER BY period_id,
            customer_id,
            item_id),
            
-- calculation by orders 

CTE2 AS (           
SELECT period_id,
       item_id,
       COUNT (CASE WHEN num_orders = 1 THEN num_orders END) AS new_customers_count,
             SUM (CASE WHEN num_orders = 1 THEN sum_payment_amount END) AS new_customers_revenue,
                 COUNT (CASE WHEN num_orders > 1 THEN num_orders END) AS returning_customers_count,
                       SUM (CASE WHEN num_orders > 1 THEN sum_payment_amount END) AS returning_customers_revenue
FROM CTE1
GROUP BY period_id,
         item_id
ORDER BY period_id,
         item_id),
-- calculate: number of customers who issued a refund and return revenue
CTE3 AS
(SELECT period_id,
          item_id,
          COUNT (DISTINCT CASE
                              WHEN status = 'refunded' THEN customer_id END) AS refunded_customer_count,
                COUNT (CASE WHEN status = 'refunded' THEN customer_id END) AS customers_refunded
   FROM CTE
   GROUP BY period_id,
            item_id
   ORDER BY period_id,
            item_id),            

CTE4 AS
(SELECT DISTINCT period_id,
                   item_id
   FROM CTE
   ORDER BY period_id,
            item_id)
-- result            
SELECT CTE2.new_customers_count,
       CTE2.returning_customers_count,
       CTE3.refunded_customer_count,
       CTE4.period_id,
       CTE4.item_id,
       CTE2.new_customers_revenue,
       CTE2.returning_customers_revenue,
       CTE3.customers_refunded
FROM CTE4
LEFT JOIN CTE2 ON (CTE4.period_id = CTE2.period_id AND CTE4.item_id = CTE2.item_id)
LEFT JOIN CTE3 ON (CTE4.period_id = CTE3.period_id AND CTE4.item_id = CTE3.item_id)
order by CTE4.period_id, CTE4.item_id;