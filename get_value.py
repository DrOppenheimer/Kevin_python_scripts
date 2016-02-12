def get_value(my_key, my_dictionary):
    if my_dictionary.has_key(my_key):
        my_value = my_dictionary.get(my_key)
        print(my_value)
    else:
        print(my_key + " : does not exist")
