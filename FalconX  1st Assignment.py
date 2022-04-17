#!/usr/bin/env python
# coding: utf-8

# # Importing three Datasets

# In[154]:


import pandas as pd
from functools import reduce
#The 2nd, 3rd, 4th and 5th columns are:
#Traded Price, Traded Size, Traded Notional, Timestamp

# creating dataframe from BTCUSDT data

df1 = pd.read_table('BTCUSDT-trades-2022-03-01.csv', sep=',')
df2 = pd.read_table('BTCUSDT-trades-2022-03-02.csv', sep=',')
df3 = pd.read_table('BTCUSDT-trades-2022-03-03.csv', sep=',')


# # Working on First dataset

# In[155]:


#looking for data and columns
df1.head(1)


# In[156]:


# Dropping the non essential columns
df1.drop(columns=['1274557543','True','True.1'],inplace=True)


# In[157]:


#Got the required columns
df1.head(5)


# In[158]:


# Renaming the columns 
df1.rename(columns={'43160.00000000':'Traded Price','0.00056000':'Traded Size','24.16960000':'Traded Notional','1646092800000':'Timestamp'},inplace=True)


# In[159]:


df1.head(1)


# In[160]:


df1.isnull().sum()


# # Tick level data with respect to Traded price

# In[161]:



nb_minutes = 1
df1TP = (
    df1.set_index(pd.to_datetime(df1['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Price'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
      .unstack(level=0) # to make it a dataframe
)


# In[162]:


print(df1TP)


# # Tick level data with respect to Traded size

# In[163]:



nb_minutes = 1 
df1TS = (
    df1.set_index(pd.to_datetime(df1['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Size'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
     .unstack(level=0) # to make it a dataframe
)


# In[164]:


df1TS


# # Tick level with respect to Traded Notional

# In[165]:



nb_minutes = 1 
df1TN = (
    df1.set_index(pd.to_datetime(df1['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Notional'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
     .unstack(level=0) # to make it a dataframe
)


# In[166]:


df1TN


# In[167]:


DF1=pd.concat([df1TP, df1TS, df1TN],axis=1,keys=['Traded Price','Traded Size','Traded Notional'])
DF1


# # second Dataset 

# In[168]:


df2.head(1)


# In[169]:


# Dropping the non essential columns
df2.drop(columns=['1276424414','True','True.1'],inplace=True)


# In[170]:


df2.head(5)


# In[171]:


# Renaming the columns 
df2.rename(columns={'44421.20000000':'Traded Price','0.00097000':'Traded Size','43.08856400':'Traded Notional','1646179200000':'Timestamp'},inplace=True)


# In[172]:


df2.head(1)


# # Tick level data with respect to Traded price

# In[173]:


nb_minutes = 1
df2TP = (
    df2.set_index(pd.to_datetime(df2['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Price'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
      .unstack(level=0) # to make it a dataframe
)


# In[174]:


df2TP


# # Tick level data with respect to Traded size

# In[175]:



nb_minutes = 1 
df2TS = (
    df2.set_index(pd.to_datetime(df2['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Size'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
     .unstack(level=0) # to make it a dataframe
)


# In[176]:


df2TS


# # Tick level with respect to Traded Notional

# In[177]:


nb_minutes = 1 
df2TN = (
    df2.set_index(pd.to_datetime(df2['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Notional'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
     .unstack(level=0) # to make it a dataframe
)


# In[178]:


df2TN


# In[179]:


DF2=pd.concat([df2TP, df2TS, df2TN],axis=1,keys=['Traded Price','Traded Size','Traded Notional'])
DF2


# # 3rd Dataset

# In[180]:


df3.head(1)


# In[181]:


# Dropping the non essential columns
df3.drop(columns=['1278077545','False','True'],inplace=True)


# In[182]:


df3.head(5)


# In[183]:


# Renaming the columns 
df3.rename(columns={'43892.99000000':'Traded Price','0.00158000':'Traded Size','69.35092420':'Traded Notional','1646265600000':'Timestamp'},inplace=True)


# In[184]:


df3.head(1)


# # Tick level data with respect to Traded price

# In[185]:


nb_minutes = 1
df3TP = (
    df3.set_index(pd.to_datetime(df3['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Price'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
      .unstack(level=0) # to make it a dataframe
)


# In[186]:


df3TP


# # Tick level data with respect to Traded size

# In[187]:



nb_minutes = 1 
df3TS = (
    df3.set_index(pd.to_datetime(df3['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Size'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
     .unstack(level=0) # to make it a dataframe
)


# In[188]:


df3TS


# # Tick level with respect to Traded Notional

# In[189]:


nb_minutes = 1 
df3TN = (
    df3.set_index(pd.to_datetime(df3['Timestamp'], ))
      .sort_index() # to ensure the chronological order
      ['Traded Notional'].resample(f'{nb_minutes}T') 
      .agg( {'open':'first', 'high':max, 'low':min, 'close':'last'} )
     .unstack(level=0) # to make it a dataframe
)


# In[190]:


df3TN


# In[191]:


# compile the list of dataframes you want to merge
data_frames = [df3TP, df3TS, df3TN]


# In[192]:


DF3=pd.concat([df3TP, df3TS, df3TN],axis=1,keys=['Traded Price','Traded Size','Traded Notional'])


# In[193]:


DF3


# In[194]:


final_df=pd.concat([DF1,DF2,DF3],axis=1,keys=['df1','df2','df3'])


# # overall Tick level data

# In[195]:


final_df

