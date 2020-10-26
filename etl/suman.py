split_copy_list = [['Large Flavoured latte', 'Gingerbread', '2.85'], ['regular latte', 'Vanilla', '2.85'], ['water', 'Gingerbread', '2.85'], ['tea', 'Gingerbread', '2.85']]
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

if __name__ == "__main__":
    x = check_for_drink_size(split_copy_list)

    print(x)




