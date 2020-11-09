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

SELECT O.Order_id, O.Item_id, O.Time_id, I.Item_id, I.Drink_type, I.Drink_flavour, I.Price, I.Drink_size,  T.datetime
FROM Orders O 
INNER JOIN Items I
ON I.Item_id = O.Item_id
Inner JOIN Time T
ON O.Time_id = T.Time_id

SELECT O.Order_id, O.Item_id, O.Payment_id, O.Time_id, I.Location_name, I.Price, I.Drink_type, I.Drink_flavour, I.Drink_size, P.Customer_id, P.Total_amount, P.Payment_type, P.Card_number, T.datetime, T.Day_id, T.Month_id, T.Year_id
FROM Orders O 
INNER JOIN Items I
ON I.Item_id = O.Item_id
INNER JOIN Time T
ON O.Time_id = T.Time_id
INNER JOIN Payments P
ON O.Payment_id = P.Payment_id


DROP TABLE IF EXISTS Time CASCADE;
DROP TABLE IF EXISTS Payments CASCADE;
DROP TABLE IF EXISTS Customers CASCADE;
DROP TABLE IF EXISTS Day CASCADE;
DROP TABLE IF EXISTS Year CASCADE;
DROP TABLE IF EXISTS Month CASCADE;