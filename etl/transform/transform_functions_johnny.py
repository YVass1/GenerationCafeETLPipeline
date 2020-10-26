# I: {"datetime": [string], "location": [string], "customer_name": [string], "purchase": [string], "total_price": [float], "payment_method": [string], "card_number": [string]}
# O: {"datetime": [string], "location": [string], "customer_name": [string], "purchase": [string], "total_price": [float], "payment_method": [string], "card_number": [string]}

#11/10/2020 08:11,Aberdeen,Joan Pickles,"Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85",11.00,CARD,5359353452578239
#11/10/2020 08:11,Aberdeen,Ross Waits," Speciality Tea - Camomile - 1.30, Regular Filter coffee - 1.50",2.80,CASH,

mock_data = {"datetime": ["11/10/2020 08:11", "11/10/2020 08:11"], "location": ["Aberdeen", "Aberdeen"], "customer_name": ["Joan Pickles", "Ross Waits"], "purchase": ["Large Flavoured latte - Gingerbread - 2.85, Large Flavoured latte - Vanilla - 2.85, Large Latte - 2.45, Large Flavoured latte - Gingerbread - 2.85", " Speciality Tea - Camomile - 1.30, Regular Filter coffee - 1.50"], "total_price": [11.00, 2.80], "payment_method": ["CARD", "CASH"], "card_number": ["5359353452578239"]}

def clean_total_price(raw_list):
    cleaned_total_price = [float(price) for price in raw_list]
    return cleaned_total_price

# print(clean_total_price(mock_data["total_price"]))

#SQL DATETIME - format: YYYY-MM-DD HH:MI:SS
#Incoming format: 11/10/2020 08:11
def clean_datetime(raw_list):
    cleaned_datetime = []
    for datetime in raw_list:
        cleaned_datetime.append(datetime[6:10] + "-" + datetime[3:5] + "-" + datetime[0:2] + " " + datetime[11:16] + ":00")
    return cleaned_datetime

# print(clean_datetime(mock_data["datetime"]))



# Tests
def test_clean_total_price():
    # Arrange
    mock_list = [11, 2.80]
    expected_output = [float(11), float(2.80)]

    # Act
    actual_output = clean_total_price(mock_list)

    # Assert
    assert expected_output == actual_output


def test_clean_datetime():
    # Arrange
    mock_list = ["11/10/2020 08:11", "11/10/2020 08:11"]
    expected_output = ["2020-10-11 08:11:00", "2020-10-11 08:11:00"]

    # Act
    actual_output = clean_datetime(mock_list)

    # Assert
    assert expected_output == actual_output

if __name__ == "__main__":
    pass