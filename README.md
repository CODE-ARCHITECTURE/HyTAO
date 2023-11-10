(1) Workfolw Introduction
  ● Read worker and task data via module “def import_excel()”;
  ● Construct balanced tree via module "def tree_built()";
  ● Search worker and update tree in the main part, which begin with "for h in range():"
  ● Output the assignment results via the function "result = [complete_task, rate, total_utility, M1, M2, running_time]";

(2) Module Introduction
  ● def import_excel()
  Utillized to read worker and task data: 
  the information of a worker is shown below {'location':'','speed':'','schedule':''}, 
  and the information of a task contains the following sections {'sl':'','at':'','st':'','dl':'','pt':'','budget':''};
 
  ● def tree_built()
  Through this module, we can construct a balanced tree based on the idle time slots of workers.
  
  ●def search()
  This module is utilized to search the maximum subtree

  ●class Node()
  This module is used to insert the new node into the tree

  ●the main part "for h in range(task_number1):"
  In this part, we first check whether the existing internal workers are eligible for task processing. If there are eligible internal workers, 
  we compute "utility" and update the tree through "root3.insert()". If there are no eligible internal workers, we proceed to search for 
  external workers as an alternative solution. 
