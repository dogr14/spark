import sys

# 列表推导式 过滤掉长度小于或等于3的字符串列表,并将剩下的转换成大写字母
names = ['Bob','Tom','alice','Jerry','Wendy','Smith']

new_names = [name.upper() for name in names if len(name)>3 ]

print(new_names)


# 字典推导式 使用字母串并根据其长度创建字典

listd = ['Google','runna','aaa','bbb']

new_dict = {key:len(key) for key in listd}

print(new_dict)

# 集合推导式 计算数字1,2,3的平方数

setnew = {i**2 for i in range(3)}

print(setnew)

# 元组表达式的用法与列表推导式完全相同 只是使用圆括号()