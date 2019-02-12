import weaver
print(weaver.dataset_names())

# ['tables-a-b-columns-a',
# 'tables-a-b-columns-a-custom',
# 'tables-a-b-d-columns-a',
# 'tables-a-c-columns-a-b',
# 'tables-a-c-e-columns-a-b']

h = weaver.join_postgres("tables-a-b-columns-a", database='testdb')
h.to_csv()

