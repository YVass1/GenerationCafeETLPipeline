def card_num_format(card_num_list):
    num_star = [] 
    for num in card_num_list:
        if not num or num.isspace():
            num_star.append(None)
        elif num.isnumeric() and 16 >= len(num) >= 12:
            format_card = num[-4:].rjust(len(num), "*")
            num_star.append(format_card)
        else:
            print(f"Card Number does not meet requirement: {num}")

    return num_star

if __name__ == "__main__":
    card_num_var = card_num_format(['44234234238934325', '4365645654645645', ''])
    print(card_num_var)