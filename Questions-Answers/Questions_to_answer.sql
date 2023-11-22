--QUESTION 1 HOW MANY STORES DOES THE BUSINESS HAVE AND IN WHICH COUNTRIES?
SELECT country_code, COUNT(store_code) as no_of_stores from dim_store_details
GROUP BY country_code
ORDER BY no_of_stores DESC

--QUESTION 2 Which Locations Currently have the most stores?
SELECT locality, COUNT(store_code) as total_no_of_stores from dim_store_details
GROUP BY locality
ORDER BY total_no_of_stores DESC
LIMIT 7

--QUESTION 3 Which month produce the average highest cost of sales typically
 SELECT ROUND(SUM(public.orders_table.product_quantity * public.dim_products.product_price_in_£::Numeric),2) as total_sales, 
 public.dim_date_times.month from orders_table
 JOIN public.dim_products on public.dim_products.product_code = public.orders_table.product_code
 JOIN public.dim_date_times on public.dim_date_times.date_uuid = public.orders_table.date_uuid
 GROUP BY public.dim_date_times.month
 ORDER BY total_sales DESC

--QUESTION 4 How many sales are coming from online
 SELECT COUNT(public.orders_table.store_code) AS number_sales,
 SUM(public.orders_table.product_quantity) AS product_quantity_count,
 CASE
    WHEN store_type = 'Web Portal' then 'Web'
    ELSE 'Offline'
 END AS online_sales from public.dim_store_details
 JOIN public.orders_table on public.orders_table.store_code = public.dim_store_details.store_code
 GROUP BY online_sales

--QUESTION 5 What percentage of sales came from each store type?
WITH total_revenue AS (SELECT SUM(orders_table.product_quantity * dim_products.product_price_in_£::NUMERIC) as total_revenue_metric from dim_store_details
JOIN public.orders_table on public.orders_table.store_code = public.dim_store_details.store_code
JOIN public.dim_products on public.dim_products.product_code = public.orders_table.product_code)

SELECT store_type,
SUM(orders_table.product_quantity * dim_products.product_price_in_£::NUMERIC) as total_sales,
ROUND((SUM(orders_table.product_quantity * dim_products.product_price_in_£::NUMERIC)/(SELECT total_revenue_metric from total_revenue)*100),2) AS percentage_total from dim_store_details
JOIN public.orders_table on public.orders_table.store_code = public.dim_store_details.store_code
JOIN public.dim_products on public.dim_products.product_code = public.orders_table.product_code
GROUP BY store_type
ORDER BY percentage_total DESC

--QUESTION 6 Which month in each year produced the highest cost of sales
WITH total_sales_year_partitioned_and_month
AS (
    SELECT
          *
        , row_number() OVER (PARTITION BY year ORDER BY total_sales DESC) AS rn
    FROM (
        SELECT
              year
            , month
            , SUM(orders_table.product_quantity * dim_products.product_price_in_£::numeric) AS total_sales
        FROM PUBLIC.dim_date_times
        JOIN PUBLIC.orders_table ON PUBLIC.orders_table.date_uuid = PUBLIC.dim_date_times.date_uuid
        JOIN PUBLIC.dim_products ON PUBLIC.dim_products.product_code = PUBLIC.orders_table.product_code
        GROUP BY year, month
        ) AS total_sales_per_year_per_month
    )

SELECT
      year
    , month
    , total_sales
FROM total_sales_year_partitioned_and_month
WHERE rn = 1
ORDER BY total_sales desc

--QUESTION 7 What is our staff headcount?
SELECT SUM(staff_numbers) as total_staff_numbers, country_code from dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC

--QUESTION 8 Which German store type is selling the most?
SELECT store_type, 
SUM(orders_table.product_quantity::numeric * dim_products.product_price_in_£::numeric) as total_sales,
country_code from dim_store_details
JOIN public.orders_table on public.orders_table.store_code = public.dim_store_details.store_code
JOIN public.dim_products on public.dim_products.product_code = public.orders_table.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales 

--QUESTION 9 How quickly is the company making sales for every year?
WITH 

year_and_order_time as (SELECT year ,(make_date(year::int, month::int, day::int) || ' ' || timestamp)::TIMESTAMP  as order_time from dim_date_times
JOIN orders_table on orders_table.date_uuid = dim_date_times.date_uuid
order by year, order_time),

time_difference_of_sales as (SELECT *, LEAD(order_time, 1) 
OVER (
		PARTITION by year
		ORDER BY order_time
	) AS next_order_time, AGE( LEAD(order_time, 1) 
    OVER (
		PARTITION by year
		ORDER BY order_time
	), order_time) as time_difference_between_sales FROM year_and_order_time),
	
sales_speed_per_year as 
(SELECT sum(time_difference_between_sales)/count(time_difference_between_sales) as speed_of_sales, 
 year from time_difference_of_sales
GROUP BY YEAR)

SELECT year, 'Hours: ' || date_part('hour', speed_of_sales) || ' minutes: ' || date_part('minutes', speed_of_sales) || ' seconds: ' || date_part('seconds', speed_of_sales) as time_actually_taken from sales_speed_per_year
order by speed_of_sales desc

 
