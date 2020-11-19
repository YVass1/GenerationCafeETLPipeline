
drink_flavour_index = 1
drink_order = ('Chai Latte', 'NULL', 'Large', 260)
drink_order_list = list(drink_order)
drink_order_list.remove(drink_order_list[drink_flavour_index])
print(drink_order_list)