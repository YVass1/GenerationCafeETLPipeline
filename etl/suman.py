# raw_data = {"datetime": ["11102020081100", "11102020081100"], 
# "location": ["Aberdeen","Aberdeen"], 
# "customer_name": ["Joan Pickles", "Ross Waits"], 
# "purchase": ["Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85,  Speciality Tea - Camomile - 1.30, Regular Filter coffee - 1.50"], 
# "total_price": [11, 2.8], 
# "payment_method": ["CARD", "CASH"], 
# "card_number": ["5359353452578230", "None"]}

customer = ["Joan Pickles", "Ross Waits"]

def clean_customer_name(customer_name):
    fnamelist = []
    lnamelist = []
    for name in customer_name:
        # https://www.geeksforgeeks.org/python-string-find/
        first_name, last_name = name.strip().split()
        fnamelist.append(first_name.title())
        lnamelist.append(last_name.title())
    return (fnamelist, lnamelist)

if __name__ == "__main__":
    customer_name = clean_customer_name(customer)
    print(customer_name[0])
    print(customer_name[1])
