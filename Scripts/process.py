import multiprocessing
import os                                              
   
# Creating the tuple of all the processes which can be run in parallel
all_parallel_processes = ('Scripts/A.py', 'Scripts/B.py', 'Scripts/C.py','Scripts/D.py')                                                                                                          
                                                  
# This block of code enables us to call the script from command line.                                                                                
def execute(process):
    os.system(f'python {process}')                                       
                                                                                                            
process_pool = multiprocessing.Pool(processes = 8)                                                        
process_pool.map(execute, all_parallel_processes)