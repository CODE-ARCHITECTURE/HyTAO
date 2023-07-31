import numpy
import xlwt
import pandas as pd
import xlrd
from xlrd import xldate_as_tuple
import datetime
import timeit 
import time
import random
import csv
from copy import deepcopy
import tracemalloc

def listcopy(listname):
    temp=[]
    for i in listname:
        if  isinstance(i,list):
            temp.append(listcopy(i))
        else:
            temp.append(i)
    return temp

def search(a, t):
    l = 0
    r = len(a) - 1
    if (t<a[l]):
        return None
    elif (t>=a[r]):
        return r
    else:
        while l <= r:
            mid = int((l+r)/2)
#            print(mid)
            if t<a[mid]:
                r = mid 
            elif (a[mid]<=t and a[mid+1]>t):
                return mid
            else:
                l = mid

class Node:
    def __init__(self, data):
        self.left = None
        self.right = None
        self.data = data

    def insert(self, data):
            if data < self.data:
                if self.left is None:
                    self.left = Node(data)
                else:
                    self.left.insert(data)
            elif data > self.data:
                if self.right is None:
                    self.right = Node(data)
                else:
                    self.right.insert(data)

    def PrintTree(self):
        if self.left:
            self.left.PrintTree()
        print(self.data),
        if self.right:
            self.right.PrintTree()

    def inorderTraversal(self, root):
        res = []
        if root:
            res = self.inorderTraversal(root.left)
            res.append(root.data)
            res = res + self.inorderTraversal(root.right)
        return res

def find_neighbour_two(path):
    exchange = random.sample(range(1, len(path)-1), 2)
    temp_path = deepcopy(path)
    temp_path[exchange[0]] = path[exchange[1]]
    temp_path[exchange[1]] = path[exchange[0]]
    return temp_path

def find_neighbour_one(path):
    endpoints = random.sample(range(1, len(path)-1), 2)
    endpoints.sort()
    temp_path = deepcopy(path)
    temp_path[0]=path[endpoints[0]]
    temp_path[1]=path[endpoints[1]]
    temp_path[endpoints[0]]=path[0]
    temp_path[endpoints[1]]=path[1]
    return temp_path

def tree_built(temp_worker_tables,task_tables_1):
    tree_node=[]
    node_worker={}
    for i in range(len(temp_worker_tables)):
        workeri_idle_period={}
        sch_task=temp_worker_tables[i]['schedule']
        if (sch_task==[]):
           idle_st=0
           if (idle_st not in tree_node):
               tree_node.append(idle_st)
               node_worker[idle_st]=[i]
           else:
               array1=node_worker[idle_st]
               array1.append(i)
               node_worker[idle_st]=array1
           workeri_idle_period[0]=[[],0,0+3000000000,[]]
           temp_worker_tables[i]['idle_period']=workeri_idle_period#'添加worker i的空闲信息'{idle_period:{idel time:[idle time,end time,[task_number]]}}
        else:
            worker_sch_task={}
            for j in sch_task:
                worker_sch_task[j]=task_tables_1[j]['st']
            order_worker_sch_task=sorted(worker_sch_task.items(), key = lambda kv:(kv[1], kv[0]))
            workeri_tree_node=[]                
            if (len(order_worker_sch_task)==1):
                task_number=order_worker_sch_task[0][0]
                idle_time=task_tables_1[task_number]['st'][0]+task_tables_1[task_number]['pt'][0]
                if (order_worker_sch_task[0][1][0]==0):
                    workeri_idle_period[idle_time]=[[task_number],idle_time,idle_time+3000000000,[]]
                else:
                    workeri_idle_period[0]=[[],0,task_tables_1[task_number]['st'][0],[task_number]]
                    workeri_idle_period[idle_time]=[[task_number],idle_time,idle_time+3000000000,[]]
            else:
                if (order_worker_sch_task[0][1][0]!=0):
                    workeri_idle_period[0]=[[],0,task_tables_1[order_worker_sch_task[0][0]]['st'][0],[order_worker_sch_task[0][0]]]
                for k in range(len(order_worker_sch_task)-1):
                    task_number=order_worker_sch_task[k][0]
                    idle_time=task_tables_1[task_number]['st'][0]+task_tables_1[task_number]['pt'][0]
                    workeri_idle_period[idle_time]=[[task_number],idle_time,task_tables_1[order_worker_sch_task[k+1][0]]['st'][0],[order_worker_sch_task[k+1][0]]]
                end_task_number=order_worker_sch_task[len(order_worker_sch_task)-1][0]
                end_task_idle_time=task_tables_1[end_task_number]['st'][0]+task_tables_1[end_task_number]['pt'][0]
                workeri_idle_period[end_task_idle_time]=[[end_task_number],end_task_idle_time,end_task_idle_time+3000000000,[]]
            temp_worker_tables[i]['idle_period']=workeri_idle_period
            workeri_tree_node=list(temp_worker_tables[i]['idle_period'].keys()) 
            for t in workeri_tree_node:
               if (t not in tree_node):
                   tree_node.append(t)
                   node_worker[t]=[i]
               else:               
                   array1=node_worker[t]
                   array1.append(i)
                   node_worker[t]=array1
    return (tree_node,node_worker,temp_worker_tables)

worker_data1 = xlrd.open_workbook(r'E:\experiment-elastic\\worker_test.xlsx')
worker_table = worker_data1.sheets()[0]
worker_tables ={}
def import_excel(excel):
   i=0
   for rown in range(excel.nrows):
      array = {'location':'','speed':'','schedule':''}
      array['location'] = worker_table.row_values(rown, start_colx=0, end_colx=2)
      array['speed'] = worker_table.row_values(rown, start_colx=2, end_colx=3)
      array['schedule'] = worker_table.row_values(rown, start_colx=3, end_colx=None)
      worker_tables[i]=array
      i+=1
if __name__ == '__main__':
   import_excel(worker_table)

for i in range(len(worker_tables)):
    temp_list=[]
    for h in worker_tables[i]['schedule']:
        if h!='':
            temp_list.append(h)
    worker_tables[i]['schedule']=temp_list

worker_data3 = xlrd.open_workbook(r'E:\experiment-elastic\\worker_test3.xlsx')
worker_table3 = worker_data3.sheets()[0]
worker_tables3={}
def import_excel(excel):
   i=0
   for rown in range(excel.nrows):
      array = {'location':'','speed':'','schedule':''}
      array['location'] = worker_table3.row_values(rown, start_colx=0, end_colx=2)
      array['speed'] = worker_table3.row_values(rown, start_colx=2, end_colx=3)
      array['schedule'] = worker_table3.row_values(rown, start_colx=3, end_colx=None)
      worker_tables3[i]=array
      i+=1
if __name__ == '__main__':
   import_excel(worker_table3)

for i in range(len(worker_tables3)):
    temp_list=[]
    for h in worker_tables3[i]['schedule']:
        if h!='':
            temp_list.append(h)
    worker_tables3[i]['schedule']=temp_list

task_data1 = xlrd.open_workbook(r'E:\experiment-elastic\\task_test.xlsx')
task_table = task_data1.sheets()[0]
task_tables ={}
def import_excel(excel):
   i=0
   for rown in range(excel.nrows):
      array = {'sl':'','at':'','st':'','dl':'','pt':'','budget':''}
      array['sl'] = task_table.row_values(rown, start_colx=0, end_colx=2)
      array['at'] = task_table.row_values(rown, start_colx=2, end_colx=3)
      array['st'] = task_table.row_values(rown, start_colx=3, end_colx=4)
      array['dl'] = task_table.row_values(rown, start_colx=4, end_colx=6)
      array['pt'] = task_table.row_values(rown, start_colx=6, end_colx=7)
      array['budget'] = task_table.row_values(rown, start_colx=7, end_colx=8)
      task_tables[i]=array
      i+=1
if __name__ == '__main__':
   import_excel(task_table)

template_task_tables=deepcopy(task_tables)
optimal_result=0
optimal_task_tables=deepcopy(template_task_tables)
x=0
constant=1
constant2=1
time_limit=0

tracemalloc.start()
total_utility=0
x+=1    
template_worker_tables=deepcopy(worker_tables)
template_worker_tables3=deepcopy(worker_tables3)

(tree_node,node_worker,template_worker_tables)=tree_built(template_worker_tables,template_task_tables)
tree_node2=[]
node_worker2={}
worker_tables2={}
root=Node(0)
flag=0  
waiting_task=[]
root3=Node(tree_node[0])
for i in range(len(tree_node)):
    root3.insert(tree_node[i])
array3=root3.inorderTraversal(root3) 

task_number1=len(template_task_tables)
for h in range(task_number1): 
    root3=Node(tree_node[0])
    for i in range(len(tree_node)):
        root3.insert(tree_node[i])
    array3=root3.inorderTraversal(root3)    
    if __name__ == '__main__':
        waiting_task_number=h
        r=search(array3, template_task_tables[waiting_task_number]['st'][0])    
    node_result=[]
    for i in range(r+1):
        node_result.append(array3[i])        
    temp_tree_node_worker_order={}
    tree_node_worker_order={}
    for i in node_result:
        temp_tree_node_worker_order[i]={}
        for j in node_worker[i]:
            temp_tree_node_worker_order[i][j]=template_worker_tables[j]['idle_period'][i][2]
        tree_node_worker_order[i]=sorted(temp_tree_node_worker_order[i].items(), key=lambda kv:(kv[1], kv[0]),reverse=True)   
    l=len(tree_node_worker_order)
    utility=0
    if (l>0):  
        task_endtime=(template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0])
        for i in tree_node_worker_order:
            n=len(tree_node_worker_order[i])
            for j in range(n):
                if (task_endtime<=tree_node_worker_order[i][j][1]):
                    temp_worker=tree_node_worker_order[i][j][0]
                    temp_idletime=i
                    if (template_worker_tables[temp_worker]['idle_period'][temp_idletime][0]==[]):
                        wx1=template_worker_tables[temp_worker]['location'][0]                   
                        wy1=template_worker_tables[temp_worker]['location'][1]  
                    else:
                        temp_task1=template_worker_tables[temp_worker]['idle_period'][temp_idletime][0][0]
                        wx1=template_task_tables[temp_task1]['sl'][0]
                        wy1=template_task_tables[temp_task1]['sl'][1]
                    slx=template_task_tables[waiting_task_number]['sl'][0]
                    sly=template_task_tables[waiting_task_number]['sl'][1]
                    distance1=round((((wx1-slx)**2+(wy1-sly)**2)**(1/2)),3)
                    move_time1=round(distance1/template_worker_tables[temp_worker]['speed'][0],3)
                    start_time1=i
                    if (template_worker_tables[temp_worker]['idle_period'][temp_idletime][3]!=[]):
                        temp_task2=template_worker_tables[temp_worker]['idle_period'][temp_idletime][3][0]
                        wx2=template_task_tables[temp_task2]['sl'][0]
                        wy2=template_task_tables[temp_task2]['sl'][1]
                        dlx=template_task_tables[waiting_task_number]['dl'][0]
                        dly=template_task_tables[waiting_task_number]['dl'][1]
                        distance2=round((((wx2-dlx)**2+(wy2-dly)**2)**(1/2)),3)
                        distance0=round((((wx2-wx1)**2+(wy2-wy1)**2)**(1/2)),3)
                    else:
                        distance2=0
                        distance0=0
                    differe_distance=distance2+distance1-distance0
                    move_time2=round(distance2/template_worker_tables[temp_worker]['speed'][0],3)
                    start_time2=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
                    if ((move_time1+start_time1)<template_task_tables[waiting_task_number]['st'][0]):
                        if ((move_time2+start_time2)<tree_node_worker_order[i][j][1]):
                            if utility<template_task_tables[waiting_task_number]['budget'][0]-constant*differe_distance:
                                utility=template_task_tables[waiting_task_number]['budget'][0]-constant*differe_distance#最终插入的worker的距离
                                incert_worker=tree_node_worker_order[i][j][0]
                                incert_tree_node=i
                else:
                    break
    if (utility>1):
        template_worker_tables[incert_worker]['schedule'].append(waiting_task_number)
        pre_idle=listcopy(template_worker_tables[incert_worker]['idle_period'][incert_tree_node])
        template_worker_tables[incert_worker]['idle_period'][incert_tree_node][2]=template_task_tables[waiting_task_number]['st'][0]
        template_worker_tables[incert_worker]['idle_period'][incert_tree_node][3]=[waiting_task_number]
        new_tree_node=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
        template_worker_tables[incert_worker]['idle_period'][new_tree_node]=[[waiting_task_number],new_tree_node,pre_idle[2],pre_idle[3]]
        if (new_tree_node not in tree_node):
            tree_node.append(new_tree_node)
            node_worker[new_tree_node]=[incert_worker]
        else:
            node_worker[new_tree_node].append(incert_worker)
        root3.insert(new_tree_node)
        array3=root3.inorderTraversal(root3) 
        total_utility+=utility
    else:
        if(len(worker_tables2)!=0):
            root=Node(tree_node2[0])
            for i in range(len(tree_node2)):
                root.insert(tree_node2[i])
            array2=root.inorderTraversal(root)
            if __name__ == '__main__':
                waiting_task_number=h
                r=search(array2, template_task_tables[waiting_task_number]['st'][0])            
            node_result=[]
            for i in range(r+1):
                node_result.append(array2[i])               
            temp_tree_node_worker_order={}
            tree_node_worker_order={}
            for i in node_result:
                temp_tree_node_worker_order[i]={}
                for j in node_worker2[i]:
                    temp_tree_node_worker_order[i][j]=worker_tables2[j]['idle_period'][i][2]
                tree_node_worker_order[i]=sorted(temp_tree_node_worker_order[i].items(), key=lambda kv:(kv[1], kv[0]),reverse=True)            
            l=len(tree_node_worker_order)
            utility=0
            if (l>0):  
                task_endtime=(template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0])
                for i in tree_node_worker_order:
                    n=len(tree_node_worker_order[i])
                    for j in range(n):
                        if (task_endtime<=tree_node_worker_order[i][j][1]):
                            temp_worker=tree_node_worker_order[i][j][0]
                            temp_idletime=i
                            if (worker_tables2[temp_worker]['idle_period'][temp_idletime][0]==[]):
                                wx1=worker_tables2[temp_worker]['location'][0]                   
                                wy1=worker_tables2[temp_worker]['location'][1]  
                            else:
                                temp_task1=worker_tables2[temp_worker]['idle_period'][temp_idletime][0][0]
                                wx1=template_task_tables[temp_task1]['sl'][0]
                                wy1=template_task_tables[temp_task1]['sl'][1]
                            slx=template_task_tables[waiting_task_number]['sl'][0]
                            sly=template_task_tables[waiting_task_number]['sl'][1]
                            dlx=template_task_tables[waiting_task_number]['dl'][0]
                            dly=template_task_tables[waiting_task_number]['dl'][1]
                            distance_task=round((((slx-dlx)**2+(sly-dly)**2)**(1/2)),3)
                            distance1=round((((wx1-slx)**2+(wy1-sly)**2)**(1/2)),3)
                            move_time1=round(distance1/worker_tables2[temp_worker]['speed'][0],3)
                            start_time1=i
                            if (worker_tables2[temp_worker]['idle_period'][temp_idletime][3]!=[]):
                                temp_task2=worker_tables2[temp_worker]['idle_period'][temp_idletime][3][0]
                                wx2=template_task_tables[temp_task2]['sl'][0]
                                wy2=template_task_tables[temp_task2]['sl'][1]
                                distance2=round((((wx2-dlx)**2+(wy2-dly)**2)**(1/2)),3)
                                distance0=round((((wx2-wx1)**2+(wy2-wy1)**2)**(1/2)),3)
                            else:
                                distance2=0
                                distance0=0
                            differe_distance=distance2+distance1-distance0#计算插入任务后travel distance的变化
                            move_time2=round(distance2/worker_tables2[temp_worker]['speed'][0],3)
                            start_time2=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
                            if ((move_time1+start_time1)<template_task_tables[waiting_task_number]['st'][0]):
                                if ((move_time2+start_time2)<tree_node_worker_order[i][j][1]):
                                    if (utility<template_task_tables[waiting_task_number]['budget'][0]-constant*differe_distance-constant2*distance_task):
                                        utility=template_task_tables[waiting_task_number]['budget'][0]-constant*differe_distance-constant2*distance_task
                                        incert_worker=tree_node_worker_order[i][j][0]
                                        incert_tree_node=i
                        else:
                            break
            if (utility>0):
                
                worker_tables2[incert_worker]['schedule'].append(waiting_task_number)
                pre_idle=listcopy(worker_tables2[incert_worker]['idle_period'][incert_tree_node])
                worker_tables2[incert_worker]['idle_period'][incert_tree_node][2]=template_task_tables[waiting_task_number]['st'][0]
                worker_tables2[incert_worker]['idle_period'][incert_tree_node][3]=[waiting_task_number]
                new_tree_node=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
                worker_tables2[incert_worker]['idle_period'][new_tree_node]=[[waiting_task_number],new_tree_node,pre_idle[2],pre_idle[3]]
                add_tree_node=[new_tree_node]
                for b in add_tree_node:
                    if (b not in tree_node2):
                        tree_node2.append(b)
                        node_worker2[b]=[incert_worker]
                    else:
                        node_worker2[b].append(incert_worker)
                root.insert(new_tree_node)
                array2=root.inorderTraversal(root)
                total_utility+=utility
            else:
                utility=0
                slx=template_task_tables[waiting_task_number]['sl'][0]
                sly=template_task_tables[waiting_task_number]['sl'][1]
                dlx=template_task_tables[waiting_task_number]['dl'][0]
                dly=template_task_tables[waiting_task_number]['dl'][1]
                distance_task=round((((slx-dlx)**2+(sly-dly)**2)**(1/2)),3)
                for a in template_worker_tables3:
                    wx1=template_worker_tables3[a]['location'][0]                   
                    wy1=template_worker_tables3[a]['location'][1]
                    distance1=round((((wx1-slx)**2+(wy1-sly)**2)**(1/2)),3)
                    move_time1=round(distance1/template_worker_tables3[a]['speed'][0],3)
                    if ((move_time1+0)<template_task_tables[waiting_task_number]['st'][0]):
                        if (utility<template_task_tables[waiting_task_number]['budget'][0]-constant*distance1-constant2*distance_task):
                            utility=template_task_tables[waiting_task_number]['budget'][0]-constant*distance1-constant2*distance_task
                            incert_worker=a
                            incert_tree_node=0
                if (utility>0):           
                    worker_tables2[incert_worker]=template_worker_tables3[incert_worker]
                    worker_tables2[incert_worker]['schedule'].append(waiting_task_number)
                    worker_tables2[incert_worker]['idle_period']={}
                    worker_tables2[incert_worker]['idle_period'][incert_tree_node]=[[],0,template_task_tables[waiting_task_number]['st'][0],[waiting_task_number]]
                    new_tree_node=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
                    worker_tables2[incert_worker]['idle_period'][new_tree_node]=[[waiting_task_number],new_tree_node,3000000000,[]]
                    add_tree_node=[incert_tree_node,new_tree_node]
                    for b in add_tree_node:
                        if (b not in tree_node2):
                            tree_node2.append(b)
                            node_worker2[b]=[incert_worker]
                        else:
                            node_worker2[b].append(incert_worker)
                    root.insert(new_tree_node)
                    array2=root.inorderTraversal(root)
                    del template_worker_tables3[incert_worker]
                    total_utility+=utility
                else:
                    waiting_task.append(h)
        else:
            utility=0
            slx=template_task_tables[waiting_task_number]['sl'][0]
            sly=template_task_tables[waiting_task_number]['sl'][1]
            dlx=template_task_tables[waiting_task_number]['dl'][0]
            dly=template_task_tables[waiting_task_number]['dl'][1]
            distance_task=round((((slx-dlx)**2+(sly-dly)**2)**(1/2)),3)
            for a in template_worker_tables3:
                wx1=template_worker_tables3[a]['location'][0]                   
                wy1=template_worker_tables3[a]['location'][1]
                distance1=round((((wx1-slx)**2+(wy1-sly)**2)**(1/2)),3)
                move_time1=round(distance1/template_worker_tables3[a]['speed'][0],3)
                if ((move_time1+0)<template_task_tables[waiting_task_number]['st'][0]):
                    if (utility<template_task_tables[waiting_task_number]['budget'][0]-constant*distance1-constant2*distance_task):
                        utility=template_task_tables[waiting_task_number]['budget'][0]-constant*distance1-constant2*distance_task
                        incert_worker=a
                        incert_tree_node=0
            if (utility>0):             
                worker_tables2[incert_worker]=template_worker_tables3[incert_worker]
                worker_tables2[incert_worker]['schedule'].append(waiting_task_number)
                worker_tables2[incert_worker]['idle_period']={}
                worker_tables2[incert_worker]['idle_period'][incert_tree_node]=[[],0,template_task_tables[waiting_task_number]['st'][0],[waiting_task_number]]
                new_tree_node=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
                worker_tables2[incert_worker]['idle_period'][new_tree_node]=[[waiting_task_number],new_tree_node,3000000000,[]]
                add_tree_node=[incert_tree_node,new_tree_node]
                for b in add_tree_node:
                    if (b not in tree_node2):
                        tree_node2.append(b)
                        node_worker2[b]=[incert_worker]
                    else:
                        node_worker2[b].append(incert_worker)
                root.insert(new_tree_node)
                array2=root.inorderTraversal(root)
                del template_worker_tables3[incert_worker]
                total_utility+=utility
            else:
                waiting_task.append(h)

print('memory=',tracemalloc.get_traced_memory())
M1,M2=tracemalloc.get_traced_memory()
tracemalloc.stop()
'时间测试'
dateTime_e=time.time() 
dateTime_e=datetime.datetime.fromtimestamp(dateTime_e) 
print('total running time=',dateTime_e-dateTime_s)
running_time=(dateTime_e-dateTime_s).total_seconds()
complete_task1=0
complete_task2=0
complete_task=0
for v in template_worker_tables:
    complete_task1+=len(template_worker_tables[v]['schedule'])
for v in worker_tables2:
    complete_task2+=len(worker_tables2[v]['schedule'])
complete_task=complete_task1+complete_task2
rate=complete_task/len(task_tables)
h1=0
h2=0
for i in template_worker_tables:
    if (len(template_worker_tables[i]['schedule'])!=0):
        h1+=1
for i in worker_tables2:
    if (len(worker_tables2[i]['schedule'])!=0):
        h2+=1

result=[complete_task,rate,total_utility,M1,M2,running_time]
with open('result3_1.csv','w',newline='')as csv_file:
    writer=csv.writer(csv_file)
    for row in result:
        writer.writerow([row])
print('result=',result)
