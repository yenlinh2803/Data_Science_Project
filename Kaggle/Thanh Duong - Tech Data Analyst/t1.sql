SELECT o.user_id, u.user_name
FROM orders AS o
INNER JOIN users AS u ON u.user_id = o.user_id
WHERE o.order_amount >=
(
	SELECT percentile_cont(0.9) within group (order by order_amount asc) as percentile_90
	FROM orders
)
GROUP BY o.user_id, u.user_name
HAVING COUNT(1) >= 3
;