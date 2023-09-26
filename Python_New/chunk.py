def chunks(lst:list, n:int):
     for i in range(0,len(lst),n):
          yield lst[i:i+n]


my_lst = ['a','b','c','d','e']

print(my_lst)
print(list(chunks(my_lst,3)))

print(list(chunks(my_lst,1)))

