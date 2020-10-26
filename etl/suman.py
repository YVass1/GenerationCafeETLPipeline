split_copy_list = [['Large Flavoured latte', 'Gingerbread', '2.85'], ['regular latte', 'Vanilla', '2.85'], ['water', 'Gingerbread', '2.85'], ['tea', 'Gingerbread', '2.85']]
def check_for_drink_size(split_copy_list):
    return_list = []
    for split_info in split_copy_list:
        split_copy = split_info.copy() #['Large Flavoured latte', 'Gingerbread', '2.85']
        looking_for_space = split_copy[0].find(" ")
        length_of_string = len(split_copy[0])
        stripped_string = split_copy[0].strip()
        remove_drink_size = stripped_string[(looking_for_space+1):length_of_string]     
        if "Large " in split_copy[0].title():
            split_copy.insert(0, "Large")
            return_list.append(split_copy)
            split_copy.insert(1, remove_drink_size)
            return_list.append(split_copy[0])
        elif "Regular " in split_copy[0].title():
            split_copy.insert(0, "Regular")
            return_list.append(split_copy)
            split_copy.insert(1, remove_drink_size)
            return_list.append(split_copy[0])
        else:
            split_copy.insert(0, None)
            return_list.append(split_copy)
    return return_list 

if __name__ == "__main__":
    x = check_for_drink_size(split_copy_list)
    print(x)




