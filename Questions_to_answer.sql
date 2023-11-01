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
 