# 1234567
#
# -> reverese
# -> sum first 5 digits



num = 1234567

a = [int(i) for i in str(num)]
print(a)

num = 1234567
num = list(map(int, str(num)))
print(num)

# 0,1,2,3,4
# ln = len(num)
# num = [num[i] for i in range(num)]
# print(num)
#

rev = num[::-1]
print(rev)
print(sum(rev[:5]))
# sum=0
# for i in range(5):
#     print(rev[i])
#     sum += int(rev[i])
# print("Sum: ",sum)


# rev = list(rev)

# rev = list(map('int', rev))

# print(rev)
# print(sum(rev[:5]))

# print(sum(int(rev[:5])))