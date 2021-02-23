from itertools import chain
joy = [('000005FEBBC74AF4B34085F54551122E',), ('8B6BCC77F9C942DEABCD9CB683999059',)]

joy_flatten_list = tuple(set(list(chain.from_iterable(joy))))  # flatten list
print(joy_flatten_list)
