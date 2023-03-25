import pandas as pd

# dates = ['2023-03-09', '2023-03-10', '2023-03-11']

# all_uids = set()

# for date in dates:
#     df = pd.read_csv(f'./data/transformed_data_download/{date}.csv')
#     all_uids.union(list(df['uid']))

# df1 = pd.read_csv('./data/transformed_data_download/2023-03-09.csv')
# df2 = pd.read_csv('./data/transformed_data_download/2023-03-10.csv')
# df3 = pd.read_csv('./data/transformed_data_download/2023-03-11.csv')
# df4 = pd.read_csv('./data/transformed_data_download/2023-03-12.csv')
# df5 = pd.read_csv('./data/transformed_data_download/2023-03-13.csv')
# df6 = pd.read_csv('./data/transformed_data_download/2023-03-14.csv')
# df7 = pd.read_csv('./data/transformed_data_download/2023-03-15.csv')
df8 = pd.read_csv('./data/transformed_data_download/2023-03-16.csv')
df9 = pd.read_csv('./data/transformed_data_download/2023-03-17.csv')
df10 = pd.read_csv('./data/transformed_data_download/2023-03-18.csv')


all_uids = set().union(
    list(df8['uid']), 
    list(df9['uid']), 
    list(df10['uid'])
    )

# # print(len(df1['uid']))
# # print(len(df2['uid']))
print(len(all_uids))

# print(len(df1))
# print(df1.head())

# df1 = df1.drop_duplicates(subset='uid', keep='last')
# print(len(df1))

# df2 = df1.pivot_table(index = ['uid'], aggfunc ='size')
# print(df2)
