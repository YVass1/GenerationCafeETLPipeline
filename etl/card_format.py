def card_num_format(card_num_list):
    num_star = [] 
    for num in card_num_list:
        if len(num) == 0:
            num_star.append(None)
        else:
            format_card = num[-4:].rjust(len(num), "*")
            num_star.append(format_card)

    return num_star

if __name__ == "__main__":
    card_num_var = card_num_format(['44234234238934325', '4365645654645645', ''])
    print(card_num_var)