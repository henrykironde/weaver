import weaver
print(weaver.dataset_names())
import numpy

# f = numpy.loadtxt(open("tables_a_b_columns_a.a_b.csv", "rb"), delimiter=",",)
# ['tables-a-b-columns-a',
# 'tables-a-b-columns-a-custom',
# 'tables-a-b-d-columns-a',
# 'tables-a-c-columns-a-b',
# 'tables-a-c-e-columns-a-b']


h = weaver.join_postgres("tables-a-b-columns-a", database='testdb')
h.to_csv()

import pandas as pd
data = pd.read_csv(h.to_csv())
print(data)

g = data[sorted(data.columns)]
print(list(g))
print(data.columns)
# # import pandas as pd
# # data = pd.read_csv("tables_a_b_columns_a.a_b.csv")
# # print(data)
# #
# # import pandas as pd
# # from collections import OrderedDict
# # from datetime import date
# #
# # sales = [('a', [1, 2]),
# #          ('b', [3, 4]),
# #          ('c', [5, 6]),
# #          ('d', ['r', 's']),
# #          ('e', ['UV', 'WX']),
# #          ]
# #
# #
# sales1 = pd.DataFrame({'a': [1, 2],
# 'b': [3, 4],
# 'c': [5, 6],
# 'd': ['r', 's'],
# 'e': ['UV', 'WX']
# })
# df = sales1
# print(df)
# # # df = pd.DataFrame.from_items(sales1)
# # # print(df.equals(data))
# # # print(df)

