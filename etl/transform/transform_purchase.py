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
        split_info_list.append(drink_info.split(" - ")) #drink_info_list: ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"]

    return split_info_list #split_info_list : [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', 'Vanilla', '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]


def check_for_flavour(split_info_list):
    return_list = []
    
    for split_info in split_info_list:
        info_copy = split_info.copy()
        
        if len(info_copy) == 3:
            return_list.append(info_copy)
        else:
            info_copy.insert(1, None)
            return_list.append(info_copy)

    return return_list  #return_list: [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]

def check_for_drink_size(split_copy_list):
    return_list = []
    for split_info in split_copy_list:
        print(split_info)
        split_copy = split_info.copy() #['Large Flavoured latte', 'Gingerbread', '2.85']
        looking_for_space = split_copy[0].find(" ")
        stripped_string = split_copy[0].strip()
        remove_drink_size = stripped_string[(looking_for_space+1):]     
        if "Large " in split_copy[0].title():
            split_copy.insert(0, "Large")
            split_copy.insert(1, remove_drink_size)
            del split_copy[2]
            return_list.append(split_copy)
        elif "Regular " in split_copy[0].title():
            split_copy.insert(0, "Regular")
            split_copy.insert(1, remove_drink_size)
            del split_copy[2]
            return_list.append(split_copy)
        else:
            split_copy.insert(0, None)
            return_list.append(split_copy)
    return return_list 

def make_split_info_list(split_info_list): #split_info_list:[["Large Flavoured latte", "Gingerbread", "2.85"], ["Large Flavoured latte", "Vanilla", "2.85"], ["Large Flavoured latte", "Gingerbread", "2.85"]]
    drink_size = []
    drink_type_list = []
    drink_flavour_list = []
    drink_price_list = []
    
    for split_info in split_info_list:
        drink_size.append (split_info[0])
        drink_type_list.append(split_info[1])
        drink_flavour_list.append(split_info[2])
        drink_price_list.append(split_info[3])

    return (drink_size, drink_type_list, drink_flavour_list, drink_price_list)


def transform_purchases(purchases):
    
    list_of_dicts = []
    
    for purchase in purchases:
        new_dict = {}
        
        stripped_purchase = purchase.strip()
        drink_info_list = stripped_purchase.split(", ") #

        split_info_list = make_drink_info_list(drink_info_list)
        split_info_list_with_nones = check_for_flavour(split_info_list) #this adds None to flavour index when no flavour is provided
        split_info_with_size = check_for_drink_size(split_info_list_with_nones)
        drink_info_lists = make_split_info_list(split_info_with_size)

        drink_size_list = drink_info_lists[0]
        drink_type_list = drink_info_lists[1]
        drink_flavour_list = drink_info_lists[2]
        drink_price_list = drink_info_lists[3]

        new_dict["drink_size"] = drink_size_list
        new_dict["drink_type"] = drink_type_list
        new_dict["drink_flavour"] = drink_flavour_list
        new_dict["drink_price"] = drink_price_list

        list_of_dicts.append(new_dict)
        # print("drink_info_list")
        # print(drink_info_list)
        # print("split_info_list")
        # print(split_info_list)
        # print("split_info_list_with_nones")
        # print(split_info_list_with_nones)
        # print("split_info_with_size")
        # print(split_info_with_size)   
    return list_of_dicts


test = transform_purchases(["Large Flavoured latte - Gingerbread - 2.85", "Speciality Tea - Green - 1.30", "Regular Flavoured latte - Vanilla - 3.85", "Tea - Mint tea - 1.85", "Large Hot chocolate - 2.90", "Smoothies - 2.75"])

# print(test[0]["drink_type"])
# ["Large Flavoured latte", "2.85"]
# ["Large Flavoured latte", None, "2.85"]

# [["Large Flavoured latte", "Gingerbread", "2.85"], ["Large Flavoured latte", "Vanilla", "2.85"], ["Large Flavoured latte", "Gingerbread", "2.85"]]