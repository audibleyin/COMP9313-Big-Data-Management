# by Chen Wu z5244467

import pickle
import random



# 20w data/ 32bit hashCode/ 5w data having the same hash with query /
# other 5w data is matched with query with offset = 10

offet  = 10

sameAsQuery=list()
for _ in range(0,32):
  a=list()
  for _ in range(0, 50000):
    a.append(0)
  sameAsQuery.append(a)

matchQueryOffSet=list()
for _ in range(0,32):
    a=list()
    for _ in range(0,50000):
        a.append(random.randint(-10, 10))
    matchQueryOffSet.append(a)

otherRandom = [[random.randint(20, 40) for _ in range(32)] for _ in range(100000) ]
r = sameAsQuery + matchQueryOffSet + otherRandom
random.shuffle(r)

with open('F:/9313/COMP9313_project_1/testCases-20200713T120144Z-001/testCases/testQueryHash1.pkl', 'wb') as f:
    pickle.dump( [ 0 for _ in range(32) ], f )



# with open('testCases/testCase1.pkl', 'rb') as f:
#     showList = pickle.load(f)
#
# print( len(showList) )
# print( len(showList[0]) )


# 100w data/ 32bit hashCode/ 5w data having the same hash with query /
# other 5w data is matched with query with offset = 10

sameAsQuery = [[0 for _ in range(32)] for _ in range(50000)]
matchWithQueryOffSet10 = [[random.randint(-10, 10) for _ in range(32)] for _ in range(50000)]
otherRandom = [[random.randint(20, 40) for _ in range(32)] for _ in range(100000) ]
r = sameAsQuery + matchWithQueryOffSet10 + otherRandom
random.shuffle(r)
with open('F:/9313/COMP9313_project_1/testCases-20200713T120144Z-001/testCases/testCase1.pkl', 'wb') as f:
    pickle.dump( r, f )
with open('F:/9313/COMP9313_project_1/testCases-20200713T120144Z-001/testCases/testCase1.pkl', 'rb') as f:
    showL = pickle.load(f)

print(len(showL))
print(len(showL[0]))

sameAsQuery = [[0 for _ in range(32)] for _ in range(50000)]
matchWithQueryOffSet10 = [[random.randint(-10, 10) for _ in range(32)] for _ in range(50000)]
otherRandom = [[random.randint(20, 40) for _ in range(32)] for _ in range(900000) ]
r = sameAsQuery + matchWithQueryOffSet10 + otherRandom
random.shuffle(r)

with open('F:/9313/COMP9313_project_1/testCases-20200713T120144Z-001/testCases/testCase2.pkl', 'wb') as f:
    pickle.dump(r, f)

with open('F:/9313/COMP9313_project_1/testCases-20200713T120144Z-001/testCases/testCase2.pkl', 'rb') as f:
    showList = pickle.load(f)

print(len(showList))
print(len(showList[0]))
