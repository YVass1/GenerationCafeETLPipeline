
customer = [" joan b pickles", " Ross Waits", "Ann Vanburen", "Linda Motes", "Jerome Guinyard"]
def clean_customer_name(customer_name):
    fnamelist = []
    lnamelist = []
    for name in customer_name:
        strip_whitespace = name.strip()
        index_for_first_space = strip_whitespace.find(" ")
        name_length = len(strip_whitespace)
        first_name = strip_whitespace[0:index_for_first_space]
        last_name = strip_whitespace[(index_for_first_space + 1):name_length]
        fnamelist.append(first_name.title())
        lnamelist.append(last_name.title())
    return (fnamelist, lnamelist)

if __name__ == "__main__":
    customer_name = clean_customer_name(customer)
    print(customer_name[0])
    print(customer_name[1])




#     fnamelist = []
#     lnamelist = []
#     for name in customer_name:
#         # https://www.geeksforgeeks.org/python-string-find/
#         first_name, last_name = name.strip().split()
#         fnamelist.append(first_name.title())
#         lnamelist.append(last_name.title())
#     return (fnamelist, lnamelist)

# if __name__ == "__main__":
#     customer_name = clean_customer_name(customer)
#     print(customer_name[0])
#     print(customer_name[1])