/*
Shopee SQL Test (30 mins)

You are to write SQL statements to answer the following questions, using the 2 tables user_tab and order_tab. Sample data is present in the respectively named sheets. (100 marks)
You may use the internet for help.

Questions:

1) Write an SQL statement to count the number of users per country (5 marks)
Answer:
*/

SELECT country, COUNT(DISTINCT userid)
FROM user_tab 
GROUP BY country;


/*
2) Write an SQL statement to count the number of orders per country (10 marks)
Answer: */
SELECT country, COUNT(DISTINCT orderid)
FROM order_tab AS ot
INNER JOIN user_tab AS ut ON ot.userid = ut.userid
GROUP BY country ;


/*
3) Write an SQL statement to find the first order date of each user (10 marks)
Answer:
*/
SELECT *
FROM 
(
SELECT orderid, userid ,order_time
		row_number() OVER (PARTITION BY userid ORDER BY order_time ASC) as nb_row
FROM dm_fact.exc01_exchange_consolidation
) AS first_order
WHERE nb_row =1;


/*
4) Write an SQL statement to find the number of users who made their first order in each country, each day (25 marks)
Answer:*/
WITH orders AS (
SELECT userid, orderid, country, order_time::date as order_date, order_time
FROM order_tab AS ot
INNER JOIN user_tab AS ut ON ot.userid = ut.userid
),
first_order AS 
(
	SELECT * FROM 
	(
		SELECT userid, orderid, country, order_date, order_time
				row_number() OVER (PARTITION BY country,order_date ORDER BY order_time ASC) as nb_row
		FROM orders
	) WHERE nb_row =1
)
SELECT country, order_date, count(DISTINCT userid)
FROM first_order
GROUP BY country, order_date;


/*
5) Write an SQL statement to find the first order GMV of each user. If there is a tie, use the order with the lower orderid (30 marks)
Answer:*/
SELECT *
FROM 
(
SELECT orderid, userid , gvm, order_time
		rank() OVER (PARTITION BY userid ORDER BY order_time ASC) as rank_nb
FROM dm_fact.exc01_exchange_consolidation
) AS first_order
WHERE rank_nb =1
ORDER BY orderid ASC
Limit 1
;


/*
6) Find out what is wrong with the sample data (20 marks)
Answer:*/

I think the itemid and gvm have issue.


