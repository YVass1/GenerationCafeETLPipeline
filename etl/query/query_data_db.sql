-- SELECT t.datetime, p.Total_amount, p.Customer_id 
-- FROM Time as t JOIN Payments as P


-- Queries Item ID from Orders table, datetime from Time table.
-- Time ID field exists in Order and Time table
SELECT Orders.Item_id, Time.datetime
FROM Orders, Time
WHERE Orders.Time_id = Time.Time_id


SELECT O.Item_id, I.Drink_type, I.Price
FROM Orders O 
INNER JOIN Items I 
ON I.Item_id = O.Item_id

SELECT O.Order_id, I.Drink_type, I.Drink_flavour, T.datetime
FROM Orders O 
INNER JOIN Items I
ON I.Item_id = O.Item_id
Inner JOIN Time T
ON O.Time_id = T.Time_id