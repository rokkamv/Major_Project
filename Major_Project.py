#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


# In[23]:


colTask = ['create_timestamp', 'modify_timestamp', 'job_id', 'task_id', 'instance_num', 'status', 
           'plan_cpu', 'plan_mem']

colInstance = ['start_timestamp', 'end_timestamp', 'job_id', 'task_id', 'machineID', 'status', 'seq_no', 
               'total_seq_no', 'real_cpu_max', 'real_cpu_avg', 'real_mem_max', 'real_mem_avg']



batchTask = pd.read_csv('batch_task.csv', names=colTask, header=None)
batchInstance = pd.read_csv('batch_instance.csv', names=colInstance, header=None)


# print(batchInstance['task_id'].value_counts())

#print(batchInstance.groupby('task_id')['real_cpu_max'].max())
print(batchInstance)
print(batchTask)


# In[4]:


print(batchInstance.groupby(
   ('task_id')
).agg(
    {
         'real_cpu_max': [min, max, sum],    # Sum duration per group
         'real_cpu_avg': [min, max, sum],  # get the count of networks
    }
).head())

#print(batchInstance.groupby('task_id')['real_mem_max'].max())


# In[5]:


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


# In[6]:


merge_task_instance = pd.merge(instanceTable,
                 batchTask[['task_id', 'plan_cpu', 'plan_mem']],
                 on='task_id')
print(merge_task_instance.head())


# In[7]:


#Grouped by plan_cpu and the maximum of real_cpu_max among all the task_ids
#having same plan_cpu value

result= merge_task_instance.groupby(['plan_cpu'],as_index=False).max() 
result


# In[87]:


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


# In[114]:


plt.figure(figsize=(50,75))

timeVsplanCpuSum_ofTask=batchTask.groupby(['create_timestamp'],as_index=False).sum()

#removing negative values
timeVsplanCpuSum_ofTask=timeVsplanCpuSum_ofTask[timeVsplanCpuSum_ofTask.create_timestamp>=0] 
x=timeVsplanCpuSum_ofTask['create_timestamp']
y=timeVsplanCpuSum_ofTask['plan_cpu']
plt.subplot(311)
plt.xlabel('Time in seconds',fontsize=50)
plt.ylabel('Total Requested CPU',fontsize=50)
# plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=40)
plt.xticks(fontsize=40)
plt.title("Total requested CPU vs (Time>=0)",fontsize=60)
plt.plot(x,y)

#removing plan_cpu values>4000
timeVsplanCpuSum_ofTask=timeVsplanCpuSum_ofTask[timeVsplanCpuSum_ofTask.plan_cpu<=4000]
x=timeVsplanCpuSum_ofTask['create_timestamp']
y=timeVsplanCpuSum_ofTask['plan_cpu']
plt.subplot(312)
plt.xlabel('Time in seconds',fontsize=50)
plt.ylabel('Total Requested CPU',fontsize=50)
# plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=40)
plt.xticks(fontsize=40)
plt.title("(Total requested CPU<=4000) vs (Time>=0)",fontsize=60)
plt.plot(x,y)

#removing plan_cpu values==0 and >2500
#removing time values >2000
timeVsplanCpuSum_ofTask=timeVsplanCpuSum_ofTask[timeVsplanCpuSum_ofTask.plan_cpu>0]
# print(timeVsplanCpuSum_ofTask)
x=timeVsplanCpuSum_ofTask['create_timestamp']
y=timeVsplanCpuSum_ofTask['plan_cpu']
plt.subplot(313)
plt.axis([0,2000,0,2500])
plt.xlabel('Time in seconds',fontsize=50)
plt.ylabel('Total Requested CPU',fontsize=50)
# plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=40)
plt.xticks(fontsize=40)
plt.title("Total requested CPU(>0 and <=2500) vs Time(0-2000)",fontsize=60)
plt.plot(x,y)

plt.show()


# In[119]:


timeVsplanCpuAvg_ofTask=batchTask.groupby(['create_timestamp'],as_index=False).mean()

plt.figure(figsize=(50,75))
#removing negative values
timeVsplanCpuAvg_ofTask=timeVsplanCpuAvg_ofTask[timeVsplanCpuAvg_ofTask.create_timestamp>=0] 
x=timeVsplanCpuAvg_ofTask['create_timestamp']
y=timeVsplanCpuAvg_ofTask['plan_cpu']
plt.subplot(311)
plt.xlabel('Time in seconds',fontsize=50)
plt.ylabel('Average Requested CPU',fontsize=50)
# plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=40)
plt.xticks(fontsize=40)
plt.title("Average requested CPU vs (Time>=0)",fontsize=60)
plt.plot(x,y)

#removing plan_cpu values>250
timeVsplanCpuAvg_ofTask=timeVsplanCpuAvg_ofTask[timeVsplanCpuAvg_ofTask.plan_cpu<=250]
x=timeVsplanCpuAvg_ofTask['create_timestamp']
y=timeVsplanCpuAvg_ofTask['plan_cpu']
plt.subplot(312)
plt.xlabel('Time in seconds',fontsize=50)
plt.ylabel('Average Requested CPU',fontsize=50)
# plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=40)
plt.xticks(fontsize=40)
plt.title("(Average requested CPU<=250) vs (Time>=0)",fontsize=60)
plt.plot(x,y)

#removing plan_cpu values==0 and >150
#removing time values >2000
timeVsplanCpuAvg_ofTask=timeVsplanCpuAvg_ofTask[timeVsplanCpuAvg_ofTask.plan_cpu>0]
# print(timeVsplanCpuAvg_ofTask)
x=timeVsplanCpuAvg_ofTask['create_timestamp']
y=timeVsplanCpuAvg_ofTask['plan_cpu']
plt.subplot(313)
plt.axis([0,2000,0,150])
plt.xlabel('Time in seconds',fontsize=50)
plt.ylabel('Average Requested CPU',fontsize=50)
# plt.xticks(index,x,fontsize=10,rotation=30)
plt.yticks(fontsize=40)
plt.xticks(fontsize=40)
plt.title("Average requested CPU(>0 and <=150) vs Time(0-2000)",fontsize=60)
plt.plot(x,y)

plt.show()

