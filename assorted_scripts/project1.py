file1 = open('./file1','r')
file2 = open('./file2','r')

myStr = [ ]
myStr2 = [ ]

for item in file1:
    myStr.append(item.rstrip('\n'))

for item in file2:
    myStr2.append(item.rstrip('\n'))

print myStr
print myStr2
