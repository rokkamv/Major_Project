#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[5]:


colTask = ['create_timestamp', 'modify_timestamp', 'job_id', 'task_id', 'instance_num', 'status', 
           'plan_cpu', 'plan_mem']

colInstance = ['start_timestamp', 'end_timestamp', 'job_id', 'task_id', 'machineID', 'status', 'seq_no', 
               'total_seq_no', 'real_cpu_max', 'real_cpu_avg', 'real_mem_max', 'real_mem_avg']



batchTask = pd.read_csv('batch_task.csv', names=colTask, header=None)
batchInstance = pd.read_csv('batch_instance.csv', names=colInstance, header=None)


# print(batchInstance['task_id'].value_counts())

#print(batchInstance.groupby('task_id')['real_cpu_max'].max())


# In[70]:


print(batchInstance.groupby(
   ('task_id')
).agg(
    {
         'real_cpu_max': [min, max, sum],    # Sum duration per group
         'real_cpu_avg': [min, max, sum],  # get the count of networks
    }
).head())

#print(batchInstance.groupby('task_id')['real_mem_max'].max())


# In[22]:


instanceTable = batchInstance.groupby(
   ('task_id')
).agg(
    {
         'real_cpu_max': max
  }
)
print(instanceTable.head())

#print(batchInstance.groupby('task_id')['real_mem_max'].max())
# print(batchTask['task_id'])


# In[23]:


merge_task_instance = pd.merge(instanceTable,
                 batchTask[['task_id', 'plan_cpu', 'plan_mem']],
                 on='task_id')
print(merge_task_instance.head())


# In[44]:


#Grouped by plan_cpu and the maximum of real_cpu_max among all the task_ids
#having same plan_cpu value

result= merge_task_instance.groupby(['plan_cpu'],as_index=False).max() 
result


# In[74]:


import matplotlib.pyplot as plt
import numpy as np

plt.figure(figsize=(10,5))
x=result['plan_cpu']
y=result['real_cpu_max']

index=np.arange(len(x))
# plt.plot(x,y)
plt.bar(index,y,width=0.8)

plt.xlabel('Requested/Allocated CPU',fontsize=20)
plt.ylabel('Actual Max CPU Usage',fontsize=20)
plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=10)

plt.title("plan_cpu vs real_cpu_max",fontsize=25)

plt.show() 

