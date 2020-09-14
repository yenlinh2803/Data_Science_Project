# Import library #
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



data_train = pd.read_csv("/Users/yenlinh/Documents/GitHub/Data_Science_Project/Titanic/raw_data/train.csv")
print(data_train.head())
data_test = pd.read_csv("/Users/yenlinh/Documents/GitHub/Data_Science_Project/Titanic/raw_data/test.csv")
print(data_test.head())

data1 = data_train.copy(deep=True)
data1_value = data_test.copy(deep=True)
dataset_list = [data1,data1_value]

print(data1.describe())
print("-"*10)
print(data1.info())
print("-"*10)
print(data1.isna().sum())
