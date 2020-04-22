# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 00:12:25 2020

@author: Rameez
"""

import pandas as pd



colTask = ['create_timestamp', 'modify_timestamp', 'job_id', 'task_id', 'instance_num', 'status', 
           'plan_cpu', 'plan_mem']

colInstance = ['start_timestamp', 'end_timestamp', 'job_id', 'task_id', 'machineID', 'status', 'seq_no', 
               'total_seq_no', 'real_cpu_max', 'real_cpu_avg', 'real_mem_max', 'real_mem_avg']



batchTask = pd.read_csv('batch_task.csv', names=colTask, header=None)
batchInstance = pd.read_csv('batch_instance.csv', names=colInstance, header=None)


#print(batchInstance['task_id'].value_counts())

#print(batchInstance.groupby('task_id')['real_cpu_max'].max())

print(batchInstance.groupby(
   ('task_id')
).agg(
    {
         'real_cpu_max': [min, max, sum],    # Sum duration per group
         'real_cpu_avg': [min, max, sum],  # get the count of networks
    }
))

#print(batchInstance.groupby('task_id')['real_mem_max'].max())



