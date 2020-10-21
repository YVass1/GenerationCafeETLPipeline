# {"datetime": [str], "location": [str], "customer_name": [str], "purchase": [str], "total_price": [float], "payment_method": [str], "card_number": [str]}

# {"datetime": [str], "location": [str], "fname": [str], "lname": [str], "purchase": [dict], "total_price": [float], "payment_method": [str], "card_number": [str]}
#                                                                                       |
#                                                                                       |
#                                                                                       v

# {"drink_size": ["large", "medium"], "drink_type": ["tea", "coffee"], "drink_flavour": ["peppermint", "black"], "drink_price": [2.5, 1.75]}

#  Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85


def make_drink_info_list(drink_info_list):
    split_info_list = []

    for drink_info in drink_info_list:
        split_info_list.append(drink_info.split(" - "))

    return split_info_list


def check_for_flavour(split_info_list):
    return_list = []
    
    for split_info in split_info_list:
        info_copy = split_info.copy()
        
        if len(info_copy) == 3:
            return_list.append(info_copy)
        else:
            return_list.append(info_copy.insert(1, None))
        
    return return_list


def make_split_info_list(split_info_list):
    drink_type_list = []
    drink_flavour_list = []
    drink_price_list = []
    
    for split_info in split_info_list:
        drink_type_list.append(split_info[0])
        drink_flavour_list.append(split_info[1])
        drink_price_list.append(split_info[2])

    return (drink_type_list, drink_flavour_list, drink_price_list)


def transform_purchases(purchases):
    
    list_of_dicts = []
    
    for purchase in purchases:
        new_dict = {}
        
        stripped_purchase = purchase.strip()
        drink_info_list = stripped_purchase.split(", ")

        split_info_list = make_drink_info_list(drink_info_list)
        split_info_list_with_nones = check_for_flavour(split_info_list) #this adds None to flavour index when no flavour is provided
        drink_info_lists = make_split_info_list(split_info_list_with_nones)

        drink_type_list = drink_info_lists[0]
        drink_flavour_list = drink_info_lists[1]
        drink_price_list = drink_info_lists[2]

        new_dict["drink_type"] = drink_type_list
        new_dict["drink_flavour"] = drink_flavour_list
        new_dict["drink_price"] = drink_price_list

        list_of_dicts.append(new_dict)
        
    return list_of_dicts


test = transform_purchases(["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"])

print(test[0]["drink_type"])

# ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"]

# ["Large Flavoured latte", "2.85"]
# ["Large Flavoured latte", None, "2.85"]

# [["Large Flavoured latte", "Gingerbread", "2.85"], ["Large Flavoured latte", "Vanilla", "2.85"], ["Large Flavoured latte", "Gingerbread", "2.85"]]