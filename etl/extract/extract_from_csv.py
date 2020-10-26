import csv

def get_raw_csv_data():
    filepath = "aberdeen_11-10-2020_19-49-26.csv"
    rows = []
    
    with open(filepath, 'r') as file_:
        reader = csv.reader(file_)
        
        for line in reader:
            rows.append(str(line))

    return rows


def convert_csv_string_to_list(string):
    string_without_brackets = string.replace("['", "").replace("']", "")

    return_list = string_without_brackets.split("', '")
    return return_list


def generate_dictionary(csv_strings):
    data = {"datetime": [], "location": [], "customer_name": [], "purchase": [], "total_price": [], "payment_method": [], "card_number": []}
    
    rows = []
    
    for string in csv_strings:
        new_list = convert_csv_string_to_list(string)
        rows.append(new_list)
    
    for row in rows:
        data["datetime"].append(row[0])
        data["location"].append(row[1])
        data["customer_name"].append(row[2])
        data["purchase"].append(row[3])
        data["total_price"].append(row[4])
        data["payment_method"].append(row[5])
        data["card_number"].append(row[6])

    return data


if __name__ == "__main__":
    rows = get_raw_csv_data()
    data = generate_dictionary(rows)


#example csv row:
#['11/10/2020 08:11', 'Aberdeen', 'Joan Pickles', 'Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85', '11.00', 'CARD', '5359353452578239']

#['11/10/2020 08:11
# Aberdeen
# Joan Pickles
#  Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85
# 11.00
# CARD
# 5359353452578239']