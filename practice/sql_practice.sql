-- SELECT drink_type, day
-- FROM orders
-- JOIN items ON orders.item_id=items.item_id
-- JOIN day ON time.day_id=day.day_id
-- JOIN time ON orders.time_id=time.time_id

SELECT o.Item_id, i.Drink_type ,i.Price
FROM Orders AS o 
INNER JOIN Items AS i
WHERE i.Item_id = o.Item_id