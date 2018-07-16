import multiprocessing as mp
import operator
from multiprocessing import Manager
import glob, os, getopt, sys
from collections import OrderedDict
import time
import re
from collections import Counter
import pickle
#import numpy as np


# current time scaling with data size:
# 5 records finish in 0.21 seconds 
# 100K records finish in 0.79 seconds
# 24525K records finish in 16.55 seconds

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
			
def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)  

def find(s, ch):
    return [i for i, ltr in enumerate(s) if ltr == ch] 

def parse_line(line,prescriber_dict, cost_dict):
    # replace comma within " " with @
    line = re.sub(r'"(.*?)"', lambda x:x.group(0).replace(',','@'), line) 

    # split by , then put @ back
    dns = line.split(',')
    dns = [ i.replace('@',',').strip().strip(',').strip() for i in dns ]

    try:
        assert(len(dns) == 5)
        if dns[3] in prescriber_dict:
            prescriber_dict[dns[3]] = prescriber_dict[dns[3]] + 1; 
        else:
            prescriber_dict[dns[3]] = 1
        if dns[3] in cost_dict:
            cost_dict[dns[3]] = cost_dict[dns[3]] + float(dns[4])
        else:
            cost_dict[dns[3]] = float(dns[4])
    except:
        print(line)
        sys.exit()

# parse the chunk of data and save pkl to drive for later combining
def parse_wrapper(fname, chk_start, chk_size, jobid):
    prescriber_dict = { }
    cost_dict = { }
    with open(fname) as f:
        f.seek(chk_start)
        lines = f.read(chk_size).splitlines()
        for line in lines:
            parse_line(line,prescriber_dict, cost_dict)
    save_obj(prescriber_dict, 'sub_prescriber_dict_'+str(jobid))
    save_obj(cost_dict, 'sub_cost_dict_'+str(jobid))

def segment(fname,size=1024*1024*8):
    file_end = os.path.getsize(fname)
    with open(fname,'rb') as f:
        f.readline() # skip the header
        chk_end = f.tell()
        # read 1024*1024*8 bytes everytime
        while chk_end < file_end:
            chk_start = chk_end
            f.seek(size,1)
            f.readline()
            chk_end = f.tell()
            yield chk_start, chk_end - chk_start

def parse_file(fname, ncores):
    #init objects
    cores = mp.cpu_count() 
    pool = mp.Pool(ncores)
    jobs = []

    #create jobs
    njobs = 0
    for chkst,chksz in segment(fname):
        #print('chkst = {0}'.format(chkst))
        jobs.append(pool.apply_async(parse_wrapper,(fname,chkst,chksz,njobs)))
        njobs = njobs + 1 
    
    #wait for all jobs to finish
    for job in jobs:
        job.get()
    
    #clean up
    pool.close() 

    #clear dict memory
    prescriber_dict = Counter(dict())
    cost_dict = Counter(dict())

    for id in range(njobs):
        prescriber_dict = prescriber_dict + Counter(load_obj('sub_prescriber_dict_'+str(id)))
        cost_dict = cost_dict + Counter(load_obj('sub_cost_dict_'+str(id)))

    return (prescriber_dict, cost_dict)

def sanity_check(fname):
    success = 0
    success += os.access(fname, os.R_OK)
    success += os.path.exists(fname) 
    return success


#############################################
#    Main script starts here 
#############################################

if len(sys.argv) <= 2:
    print('Error. Need to specify input and output data as garguments.')
    sys.exit()

ifname = sys.argv[1:][0]
ofname = sys.argv[1:][1]
ncores = mp.cpu_count()

# check if input data file is accessible
if not sanity_check(ifname):
    print('{0} does not exit or can`t be accessed.'.format(ifname))
    sys.exit()

start_clock = time.time()

# parse input file and return two dictionaries
try:
    (prescriber_dict, cost_dict) = parse_file(ifname, ncores) 

except:
    print('Error. Abort parse_file({0})'.format(ifname))
    sys.exit()

# sort dict in descending order by total cost then drug name
cost_dict = sorted(cost_dict.items(), key= lambda x:(x[1],x[0]), reverse=True)         

# write two dictionaries to one output file
with open(ofname,'w') as f:
    f.write('drug_name,num_prescriber,total_cost\n')
    for key, val in cost_dict:
        tmp = '{:.5g}'.format(val) 
        f.write(key + ',' + str(prescriber_dict[key]) + ',' + tmp + '\n')
    f.flush()

# clear up temporary files
#os.remove('sub_prescriber_dict_*.pkl')  
#os.remove('sub_cost_dict_*.pkl')  
end_clock = time.time()

print('pharmacy_counting.py finishes in {0} seconds'.format(end_clock-start_clock))

