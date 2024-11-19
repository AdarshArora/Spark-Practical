##[25] Task - 1

liststr= [ "A~B,C" ,  "D~E,F" ]

rddlst = sc.parallelize(liststr)
print(rddlst.collect())

seplst = rddlst.flatMap(lambda x: x.split(',')).flatMap(lambda x: x.split('~'))
print(seplst.collect())

#Output
# ['A', 'B', 'C', 'D', 'E', 'F']