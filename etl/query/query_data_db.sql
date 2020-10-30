-- SELECT t.datetime, p.Total_amount, p.Customer_id 
-- FROM Time as t JOIN Payments as P

-- Queries Item ID from Orders table, Drink type and Price from Items table.
-- Item ID field exists in Order and Items table
SELECT o.Item_id, i.Drink_type ,i.Price
FROM Orders AS o 
INNER JOIN Items AS i
WHERE i.Item_id = o.Item_id

-- Queries Item ID from Orders table, datetime from Time table.
-- Time ID field exists in Order and Time table
SELECT o.Item_id, t.datetime
FROM Orders AS o 
INNER JOIN Time AS t
WHERE o.time_id = t.time_id
