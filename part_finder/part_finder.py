import subprocess
import time

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

    print("Job with the ID of {0} was repartitioned to {1} partition with the new ID of {2}".
          format(prev_id, submit_part, new_id))

    changed = open("repartition_log", 'a')
    changed.write(prev_part + " --> " + submit_part + "    " + prev_id + " -> " + new_id + "\n")
    changed.close()

def cancel_pending_jobs(pending):
    for job in pending:
        subprocess.Popen("scancel" + str(job[0][0]), stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)

#with open(not_executed_path, 'r') as not_executed:
    # Read the existing lines in the file
#    existing_lines = not_executed.readlines()

# Start an infinite loop to check for new lines
while True:

    pending_jobs = get_pending_jobs_list()
    cancel_pending_jobs(pending_jobs)
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
    time.sleep(5)


