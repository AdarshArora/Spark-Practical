#sum of 3 elements then other 3 and so on....
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
sum_list = []
c=1
s=0
for num in numbers:
    s+=num
    if c==3:
        sum_list.append(s)
        s=0
        c=1
        continue
    c+=1
# print(sum_list)




lst = [11, 5, 2, 9, 32]

minn = lst[0]
maxx = lst[0]

for ele in lst:
    if ele > maxx:
        maxx = ele
    if ele < minn:
        minn = ele

print('Min: ',minn)
print('Max: ',maxx)
