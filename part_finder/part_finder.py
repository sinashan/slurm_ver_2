import subprocess
import time
from datetime import datetime
import sys
import select

changed = open("squeue_logs", "w")
changed.close()

changed = open("repartition_log", "w")
changed.close()

# countes how many lines of the file it has allocated a partition to
line_counter = 0

def check_idle_partitions():
    proc = subprocess.Popen("sinfo | awk -F \' \' '{print $1, $5}\'", stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
    output = proc.stdout.read().decode("utf-8")
    first_out = output.split('\n')
    first_out = first_out[1:-1]

    all_parts = []
    for i in first_out:
        i = i.replace('*', '')
        all_parts.append(i.split(' '))

    idle_parts = []
    for part in all_parts:
        if part[0] == 'local':
            continue
        if part[1] == 'idle':
            idle_parts.append(part[0])


    return idle_parts

def get_pending_jobs_list():
    proc = subprocess.Popen("squeue -t PENDING | awk -F \' \' '{print $1, $2}\'", stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
    output = proc.stdout.read().decode("utf-8")
    first_out = output.split('\n')
    first_out = first_out[1:-1]

    jobs_pending = []

    for i in first_out:
        pending_job = i.split(' ' )
        if pending_job[1] != 'local':
            jobs_pending.append(pending_job)
    
    for job in jobs_pending:
        job.append(get_script_name(job[0]))


    jobs_pending.sort(key=lambda x: int(x[0]))
    sorted_jobs_by_id = sorted(jobs_pending, key=lambda x: int(x[0]))

    return sorted_jobs_by_id


def get_script_name(job_id):
    proc = subprocess.Popen("scontrol show jobid -dd " + str(job_id), stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
    output = proc.stdout.read().decode("utf-8")
    output = output.split('\n')

    for line in output:
        if "Command=" in line:
            return line[line.find("=") + 1:]

not_executed_path = '/mnt/nfs_share/not_executed'

def write_changed_partition_to_file(prev_part, prev_id, submit_part, output_result):
    stdout_output = output_result.stdout.read().decode("utf-8")
    new_id = stdout_output.split(' ')[-1].strip('\n')

    #print("Job with the ID of {0} was repartitioned to {1} partition with the new ID of {2}".
    #      format(prev_id, submit_part, new_id))


    current_time = datetime.now().time()
    current_date = datetime.now().date()

    formatted_time = current_time.strftime("%H:%M:%S")
    formatted_date = current_date.strftime("%d/%m/%Y")

    changed = open("squeue_logs", 'a')
    changed.write(formatted_date + '\t' + formatted_time + '\n')
    changed.write('Job ID\t\tInitial Partition\t\tNew Partition\t\tNew ID\n')
    changed.write(f"{prev_id:<10}\t\t{prev_part:<10}\t\t{submit_part:<10}\t\t{new_id:<10}\n")
    changed.write('----------------------------------------------------------------------\n')
    changed.close()

    #print("Job with the ID of {0} was repartitioned to {1} partition with the new ID of {2}".
    #      format(prev_id, submit_part, new_id))

    changed = open("repartition_log", 'a')
    changed.write(prev_part + " --> " + submit_part + "    " + prev_id + " -> " + new_id + "\n")
    changed.close()

def write_cancelled_partition(cancelled_jobs):
    cancelled = open("squeue_logs", 'a')
    current_time = datetime.now().time()
    current_date = datetime.now().date()

    formatted_time = current_time.strftime("%H:%M:%S")
    formatted_date = current_date.strftime("%d/%m/%Y")
    cancelled.write(formatted_date + '\t' + formatted_time + '\n')
    cancelled.write('Job ID\t\tInitial Partition\t\tNew Partition\t\tNew ID\n')
    
    for job in cancelled_jobs:
        submit_prev_id = job[0]
        submit_prev_part = job[1]
        repartitioning = "repartition"
        cancelled.write(f"{submit_prev_id:<10}\t\t{submit_prev_part:<10}\t\t{repartitioning:<10}\t\t-\n")
        cancelled.write('----------------------------------------------------------------------\n')
    
    cancelled.close()
        

def cancel_pending_jobs(pending):
    print(pending)
    for job in pending:
        subprocess.Popen("scancel " + str(job[0]), stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)

#with open(not_executed_path, 'r') as not_executed:
    # Read the existing lines in the file
#    existing_lines = not_executed.readlines()

pending_jobs = []
pending_jobs_len = 0
# Start an infinite loop to check for new lines
while True:

    if select.select([sys.stdin], [], [], 0)[0]:
            command = input("")
            #print("User input:", user_input)
            if 'cancel' in command:
                job_id_to_be_cancelled = command[command.find(' ')+1:]
            index = [(i, id.index(job_id_to_be_cancelled)) for i, id in enumerate(pending_jobs) if job_id_to_be_cancelled in id]
            pending_jobs.pop(index[0][0])


    pending_jobs = pending_jobs + get_pending_jobs_list()
    if len(pending_jobs) > pending_jobs_len:
        pending_jobs_len = len(pending_jobs)
        cancel_pending_jobs(pending_jobs)
        write_cancelled_partition(pending_jobs)

    #print(pending_jobs)

    idle_parts = check_idle_partitions()
    if idle_parts:
        submit_prev_id = 0
        submit_partition = ''
        submit_script = ''
        if pending_jobs:
            submit_prev_id = pending_jobs[0][0]
            submit_prev_part = pending_jobs[0][1]
            submit_partition = idle_parts[0]
            submit_script = pending_jobs[0][2]

            #print(submit_prev_id)
            #print(submit_partition)
            #print(submit_script)
    
            job_resubmit = subprocess.Popen("sbatch " + submit_script , stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
            write_changed_partition_to_file(submit_prev_part, submit_prev_id, submit_partition, job_resubmit)
            pending_jobs = pending_jobs[1:]

    # Re-open the file in read-only mode
    #with open(not_executed_path, 'r') as not_executed:
        # Read the current lines in the file
    #    current_lines = not_executed.readlines()

    
    # Check if there are any new lines
    #new_line = [line for line in current_lines if line not in existing_lines]
    #if new_line:
    #    print(f"New line detected: {new_line}")
    #    existing_lines = current_lines

    # Wait for some time before checking again
    time.sleep(1)


