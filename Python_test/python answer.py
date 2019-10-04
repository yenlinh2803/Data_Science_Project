print('hello world!')

import pandas as pd
import numpy as np
data = pd.read_excel ("Test_Pandas.xlsx", sheet="Sheet")
data.head()

data = pd.read_csv("Test_Pandas.csv", sep=",", encoding= 'utf-8')
data.head()
columns = data.columns
# Number unique shops:
nb_shop = data['Shopid'].nuqniue

#cb_option - 1 indicates the product is sold by a cross border shop
#is_preferred - 1 indicates the product is sold by a preferred shop
#How many unique preferred and cross border shops are in the dataset?
nb_shop_pre_cb = data.loc[(data['cb_option']==1) & (data['is_preferred']==1),:]
nb_shop_pre_cb = nb_shop_pre_cb['Shopid'].nuqniue

# How many products have zero sold count?
nb_product_sold = data.loc[data['sold_count']==0,:]
nb_product_sold = nb_product_sold['Itemid'].nuqniue

#How many products were created in the year 2018?
nb_product_2018 = data.loc[(data['item_creation_date'] >= '2018-01-01 00:00:00') & (data['item_creation_date'] < '2019-01-01 00:00:00'),:]
nb_product_2018 = nb_product_2018['Itemid'].nuqniue


#Top 3 Preferred shops’ shopid that have the largest number of unique products
top_3_pre = data.loc[(data['is_preferred']==1),:]
top_3_pre = top_3_pre.groupby['shopid']['Itemid'].nuqniue
top_3_pre = top_3_pre.sort_values('Itemid', ascending=False )
top_3_pre = top_3_pre.sample(3)

#Top 3 Categories that have the largest number of unique cross-border products
top_3_cat_cb = data.loc[(data['cb_option']==1),:]
top_3_cat_cb = top_3_cat_cb.groupby['category ']['Itemid'].nuqniue
top_3_cat_cb = top_3_cat_cb.sort_values('Itemid', ascending=False )
top_3_cat_cb = top_3_cat_cb.sample(3)


#Find number of products that have more than 3 variations (do not include products with 3 or fewer variations)
#tem_variation - stores variations of a product (e.g. different colours or sizes, in the format like {variation 1 name: variation 1 price, variation 2 name: variation 2 price})



#Use pandas function to identify duplicated listings within each shop (If listing A and B in shop S have the exactly same product title, product detailed description, and price, both listing A and B are considered as duplicated listings)
shop_duplicate = data[data.duplicated(['shopid','Itemid','item_description','price'])]


#Mark those duplicated listings with True otherwise False and store the marking result in a new column named “is_duplicated” 
#Find duplicate listings that has less than 2 sold count and store the result in a new excel file named “duplicated_listings.xlsx”
#Find the preferred shop shopid that have the most number of duplicated listings





