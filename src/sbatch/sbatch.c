/*****************************************************************************\
 *  sbatch.c - Submit a Slurm batch script.$
 *****************************************************************************
 *  Copyright (C) 2006-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Copyright (C) 2010-2017 SchedMD LLC.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Christopher J. Morrone <morrone2@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>               /* MAXPATHLEN */
#include <sys/resource.h> /* for RLIMIT_NOFILE */

#include "slurm/slurm.h"

#include "src/common/cli_filter.h"
#include "src/common/cpu_frequency.h"
#include "src/common/env.h"
#include "src/common/node_select.h"
#include "src/common/pack.h"
#include "src/common/plugstack.h"
#include "src/common/proc_args.h"
#include "src/common/read_config.h"
#include "src/common/slurm_auth.h"
#include "src/common/slurm_rlimits_info.h"
#include "src/common/xstring.h"
#include "src/common/xmalloc.h"

#include "src/sbatch/opt.h"

#include "src/common/strlcpy.h"
#include "src/common/uid.h"

#include "src/scontrol/scontrol.h"

#define MAX_RETRIES 15
#define MAX_WAIT_SLEEP_TIME 32

static void  _add_bb_to_script(char **script_body, char *burst_buffer_file);
static void  _env_merge_filter(job_desc_msg_t *desc);
static int   _fill_job_desc_from_opts(job_desc_msg_t *desc);
static void *_get_script_buffer(const char *filename, int *size);
static int   _job_wait(uint32_t job_id);
static char *_script_wrap(char *command_string);
static void  _set_exit_code(void);
static void  _set_prio_process_env(void);
static int   _set_rlimit_env(void);
static void  _set_spank_env(void);
static void  _set_submit_dir_env(void);
static int   _set_umask_env(void);

int quiet_flag = 0;	/* quiet=1, verbose=-1, normal=0 (added by sinashan) */
int exit_code = 0;	/* scontrol's exit code, =1 on any error at any time (added by sinashan) */
int one_liner = 0;	/* one record per line if =1 (added by sinashan) */
partition_info_msg_t *old_part_info_ptr = NULL; /* (added by sinashan)  */ 
int all_flag = 0;	/* display even hidden partitions (added by sinashan) */
partition_info_t *part_ptr = NULL; /* added by Sinashan */
partition_info_msg_t *part_info_ptr = NULL; /* added by Sinashan */


/* added by Sinashan. General info about the setup */
const double AVERAGE_HIT_RATIO = 0.5;
const double SSD_BW = 500;	// MB/s
const double HDD_BW = 800;	// MB/s
const int SSD_IOPS = 95000;	// IO/s
const int HDD_IOPS = 2000;	// IO/s
const double SSD_BLK_SIZE = 4;	// KB
const double HDD_BLK_SIZE = 0.5;	// KB
const int CACHE_PART_SIZE = 500; // GB
const uint32_t EXECUTION_TIME = 200;	// minutes fot 500 GB dataset size

/* App Info */
/* Tensorflow */
const double tensorflow_hit = 0.5;
int tensorflow_average_exec_time = 1;
int tensorflow_io_type = 1;	/* 1: seq, 0: rand */
const double tensorflow_avg_read_hit = 0.5;
int tensorflow_dataset_size = 500;
int tensorflow_normalized_io = 1;
/* Pytorch */
const double pytorch_hit = 0.5;
int pytorch_average_exec_time = 1;
int pytorch_io_type = 0;	/* 1: seq, 0: rand */
const double pytorch_avg_read_hit = 0.5;
int pytorch_dataset_size = 500;
int pytorch_normalized_io = 1;
/* OpenCV */
const double opencv_hit = 0.5;
int opencv_average_exec_time = 1;
int opencv_io_type = 1;	/* 1: seq, 0: rand */
const double opencv_avg_read_hit = 0.9;
int opencv_dataset_size = 500;
int opencv_normalized_io = 1;
/* Python */
const double python_hit = 0.5;
int python_average_exec_time = 1;
int python_io_type = 0;	/* 1: seq, 0: rand */
const double python_avg_read_hit = 0.5;
int python_dataset_size = 500;
int python_normalized_io = 1;
/* FIO Rand zipf */
const double fio_zipf_rand_hit = 0.91;
int fio_zipf_rand_average_exec_time = 425; /* min */
int fio_zipf_rand_io_type = 0;	/* 1: seq, 0: rand */
const double fio_zipf_rand_avg_read_hit = 0.91;
int fio_zipf_rand_dataset_size = 200;
int fio_zipf_rand_normalized_io = 1;
/* FIO Rand no zipf */
const double fio_rand_hit = 0.18;
int fio_rand_average_exec_time = 710;	/* min */
int fio_rand_io_type = 0;	/* 1: seq, 0: rand */
const double fio_rand_avg_read_hit = 0.18;
int fio_rand_dataset_size = 200;
int fio_rand_normalized_io = 1;
/* FIO Seq */
const double fio_seq_hit = 0.07;
int fio_seq_average_exec_time = 91;	/* min */
int fio_seq_io_type = 1;	/* 1: seq, 0: rand */
const double fio_seq_avg_read_hit = 0.07;
int fio_seq_python_dataset_size = 200;
int fio_seq_normalized_io = 1;

int fio_count = 0;
char* fio_test = "";

/* used to take care of what the original partition in the script was */
char* original_partition;
char* original_job_name;

/* how many base (SAN_HDD) partitions are we going to have? */
int number_of_base_parts = 1;
int number_of_cache_parts = 1;
int number_of_local_parts = 1;

/* add the number of different partitions (base + cache + local) */
char* parts_status[3][2][256];

/* Famous datasets */
char* datasets[11] = {
	"COCO",
	"HowTO100M",
	"LIReC",
	"MIT Places Audio 400K",
	"MIT Places Images",
	"MPII Human Pose",
	"Spoken COCO 600K",
	"Spoken ObjectNet",
	"UCF101",
	"Zero Speech",
	"Imagenet"
};


int main(int argc, char **argv)
{
	log_options_t logopt = LOG_OPTS_STDERR_ONLY;
	job_desc_msg_t *desc = NULL, *first_desc = NULL;
	submit_response_msg_t *resp = NULL;
	char *script_name;
	char *script_body;
	char **het_job_argv;
	int script_size = 0, het_job_argc, het_job_argc_off = 0, het_job_inx;
	int i, rc = SLURM_SUCCESS, retries = 0, het_job_limit = 0;
	bool het_job_fini = false;
	List job_env_list = NULL, job_req_list = NULL;
	sbatch_env_t *local_env = NULL;
	bool quiet = false;

	/* create an array for the base partitions */
	char **base_parts = malloc(number_of_base_parts * sizeof(char*));

	for (int i = 0; i < number_of_base_parts; i++){
		char *temp = malloc(10 * sizeof(char));
		if (i == 0)
			base_parts[i] = "base";
		else{
			sprintf(temp, "base%d", i);
			base_parts[i] = temp;
		}
	}


	/* added by Sinashan */
	FILE *ds_store;
	ds_store = fopen("jobid_dataset", "a");
	fclose(ds_store);

	slurm_job_info_t *job_ptr;	/* added by Sinashan */
	job_info_msg_t *resps = NULL;	/* added by Sinashan */

	/* force line-buffered output on non-tty outputs */
	if (!isatty(STDOUT_FILENO))
		setvbuf(stdout, NULL, _IOLBF, 0);
	if (!isatty(STDERR_FILENO))
		setvbuf(stderr, NULL, _IOLBF, 0);

	slurm_conf_init(NULL);
	log_init(xbasename(argv[0]), logopt, 0, NULL);

	_set_exit_code();
	if (spank_init_allocator() < 0) {
		error("Failed to initialize plugin stack");
		exit(error_exit);
	}

	/* Be sure to call spank_fini when sbatch exits
	 */
	if (atexit((void (*) (void)) spank_fini) < 0)
		error("Failed to register atexit handler for plugins: %m");

	script_name = process_options_first_pass(argc, argv);


	/* Preserve quiet request which is lost in second pass */
	quiet = opt.quiet;

	/* reinit log with new verbosity (if changed by command line) */
	if (opt.verbose || opt.quiet) {
		logopt.stderr_level += opt.verbose;
		logopt.stderr_level -= opt.quiet;
		logopt.prefix_level = 1;
		log_alter(logopt, 0, NULL);
	}

	if (sbopt.wrap != NULL) {
		script_body = _script_wrap(sbopt.wrap);
	} else {
		script_body = _get_script_buffer(script_name, &script_size);
	}
	if (script_body == NULL)
		exit(error_exit);

	het_job_argc = argc - sbopt.script_argc;
	het_job_argv = argv;
	for (het_job_inx = 0; !het_job_fini; het_job_inx++) {
		bool more_het_comps = false;
		init_envs(&het_job_env);
		process_options_second_pass(het_job_argc, het_job_argv,
					    &het_job_argc_off, het_job_inx,
					    &more_het_comps, script_name ?
					    xbasename (script_name) : "stdin",
					    script_body, script_size);
		if ((het_job_argc_off >= 0) &&
		    (het_job_argc_off < het_job_argc) &&
		    !xstrcmp(het_job_argv[het_job_argc_off], ":")) {
			/* het_job_argv[0] moves from "salloc" to ":" */
			het_job_argc -= het_job_argc_off;
			het_job_argv += het_job_argc_off;
		} else if (!more_het_comps) {
			het_job_fini = true;
		}

		/*
		 * Note that this handling here is different than in
		 * salloc/srun. Instead of sending the file contents as the
		 * burst_buffer field in job_desc_msg_t, it will be spliced
		 * in to the job script.
		 */
		if (opt.burst_buffer_file) {
			Buf buf = create_mmap_buf(opt.burst_buffer_file);
			if (!buf) {
				error("Invalid --bbf specification");
				exit(error_exit);
			}
			_add_bb_to_script(&script_body, get_buf_data(buf));
			free_buf(buf);
		}

		if (spank_init_post_opt() < 0) {
			error("Plugin stack post-option processing failed");
			exit(error_exit);
		}

		if (opt.get_user_env_time < 0) {
			/* Moab doesn't propagate the user's resource limits, so
			 * slurmd determines the values at the same time that it
			 * gets the user's default environment variables. */
			(void) _set_rlimit_env();
		}

		/*
		 * if the environment is coming from a file, the
		 * environment at execution startup, must be unset.
		 */
		if (sbopt.export_file != NULL)
			env_unset_environment();

		_set_prio_process_env();
		_set_spank_env();
		_set_submit_dir_env();
		_set_umask_env();
		if (local_env && !job_env_list) {
			job_env_list = list_create(NULL);
			list_append(job_env_list, local_env);
			job_req_list = list_create(NULL);
			list_append(job_req_list, desc);
		}
		local_env = xmalloc(sizeof(sbatch_env_t));
		memcpy(local_env, &het_job_env, sizeof(sbatch_env_t));
		desc = xmalloc(sizeof(job_desc_msg_t));
		slurm_init_job_desc_msg(desc);
		if (_fill_job_desc_from_opts(desc) == -1)
			exit(error_exit);
		if (!first_desc)
			first_desc = desc;
		if (het_job_inx || !het_job_fini) {
			set_env_from_opts(&opt, &first_desc->environment,
					  het_job_inx);
		} else
			set_env_from_opts(&opt, &first_desc->environment, -1);
		if (!job_req_list) {
			desc->script = (char *) script_body;
		} else {
			list_append(job_env_list, local_env);
			list_append(job_req_list, desc);
		}
	}
	het_job_limit = het_job_inx;
	if (!desc) {	/* For CLANG false positive */
		error("Internal parsing error");
		exit(1);
	}

	if (job_env_list) {
		ListIterator desc_iter, env_iter;
		i = 0;
		desc_iter = list_iterator_create(job_req_list);
		env_iter  = list_iterator_create(job_env_list);
		desc      = list_next(desc_iter);
		while (desc && (local_env = list_next(env_iter))) {
			set_envs(&desc->environment, local_env, i++);
			desc->env_size = envcount(desc->environment);
		}
		list_iterator_destroy(env_iter);
		list_iterator_destroy(desc_iter);

	} else {
		set_envs(&desc->environment, &het_job_env, -1);
		desc->env_size = envcount(desc->environment);
	}
	if (!desc) {	/* For CLANG false positive */
		error("Internal parsing error");
		exit(1);
	}

	/*
	 * If can run on multiple clusters find the earliest run time
	 * and run it there
	 */
	if (opt.clusters) {
		if (job_req_list) {
			rc = slurmdb_get_first_het_job_cluster(job_req_list,
					opt.clusters, &working_cluster_rec);
		} else {
			rc = slurmdb_get_first_avail_cluster(desc,
					opt.clusters, &working_cluster_rec);
		}
		if (rc != SLURM_SUCCESS) {
			print_db_notok(opt.clusters, 0);
			exit(error_exit);
		}
	}

	if (sbopt.test_only) {
		if (job_req_list)
			rc = slurm_het_job_will_run(job_req_list);
		else
			rc = slurm_job_will_run(desc);

		if (rc != SLURM_SUCCESS) {
			slurm_perror("allocation failure");
			exit(1);
		}
		exit(0);
	}

	while (true) {
		static char *msg;
		if (job_req_list)
			rc = slurm_submit_batch_het_job(job_req_list, &resp);
		else
			rc = slurm_submit_batch_job(desc, &resp);
		if (rc >= 0)	
			break;
		if (errno == ESLURM_ERROR_ON_DESC_TO_RECORD_COPY) {
			msg = "Slurm job queue full, sleeping and retrying";
		} else if (errno == ESLURM_NODES_BUSY) {
			msg = "Job creation temporarily disabled, retrying";
		} else if (errno == EAGAIN) {
			msg = "Slurm temporarily unable to accept job, "
			      "sleeping and retrying";
		} else
			msg = NULL;
		if ((msg == NULL) || (retries >= MAX_RETRIES)) {
			error("Batch job submission failed: %m");
			exit(error_exit);
		}

		if (retries)
			debug("%s", msg);
		else if (errno == ESLURM_NODES_BUSY)
			info("%s", msg); /* Not an error, powering up nodes */
		else
			error("%s", msg);
		slurm_free_submit_response_response_msg(resp);
		sleep(++retries);
	}

	if (!resp) {
		error("Batch job submission failed: %m");
		exit(error_exit);
	}

	print_multi_line_string(resp->job_submit_user_msg, -1, LOG_LEVEL_INFO);

	/* run cli_filter post_submit */
	for (i = 0; i < het_job_limit; i++)
		cli_filter_g_post_submit(i, resp->job_id, NO_VAL);
		
	if (!quiet) {
		if (!sbopt.parsable) {
			//printf("Submitted batch job %u", resp->job_id);
			if (working_cluster_rec)
				printf(" on cluster %s",
				       working_cluster_rec->name);
			//printf("\n");
		} else {
			printf("%u", resp->job_id);
			if (working_cluster_rec)
				printf(";%s", working_cluster_rec->name);
			printf("\n");
		}
	}

	if (sbopt.wait)
		rc = _job_wait(resp->job_id);

	/* added by Sinashan */
	FILE *changed_partition;	/* keeps track of which job ID was repartitioned */
	uint32_t alternate_jobid;	/* a job id in case the current one needs to be cancelled */
	int dataset_cache_not_execute=0;	/* not needed for now */
	sleep(2);	/* this sleep is needed to make sure sbatch takes effect */
	slurm_load_job(&resps, resp->job_id, 0);	/* loads job info */
	job_ptr = resps->job_array;	/* get job info */
	alternate_jobid = job_ptr->job_id + 1;	/* in case the current job needs to be cancelled */
	scontrol_print_part(NULL);	/* slurm function to fill the job info structure */
        
	//slurm_sprint_job_info(job_ptr, 0);
	printf("Job State: %s\n", job_state_string(job_ptr->job_state));

	/* job states: RUNNING, PENDING, FAILED (something was wrong) */
	if(!strcmp("PENDING", job_state_string(job_ptr->job_state))){

		printf("Job ID: %d\n", job_ptr->job_id);
		FILE *not_executed;
		not_executed = fopen("not_executed", "a");
		fprintf(not_executed, "%d\t%s\n", job_ptr->job_id , argv[1]);

		/* slurm function to kill the job, takes job ID as argument */
		//slurm_kill_job(resp->job_id, SIGKILL, KILL_JOB_BATCH);
		//ds_store = fopen("jobid_dataset", "r");
		/*char* temp_part = job_ptr->partition;
		printf("Partition %s cannot be used because it's full.\n", job_ptr->partition);
		slurm_kill_job(resp->job_id, SIGKILL, KILL_JOB_BATCH);
		for (i = 0; i < part_info_ptr->record_count; i++) {
			char part_first_letter[1];
			sprintf(part_first_letter, "%.*s", 1, part_ptr[i].name);
			if(!strcmp(temp_part, part_ptr[i].name)) {
				if(i == part_info_ptr->record_count-1) {
					dataset_cache_not_execute = 1;
					printf("No free partition available\n");
					break;
				}
				continue;
			}

			if (desc->dataset_size > 0)
				i*f (strcmp(part_first_letter, "c"))
					continue;

			desc->partition = part_ptr[i].name;
			slurm_submit_batch_job(desc, resp);
			sleep(3);
			slurm_load_job(&resps, alternate_jobid, 0);
			job_ptr = resps->job_array;

			if(!strcmp("RUNNING", job_state_string(job_ptr->job_state))){
				printf("Job cancelled. New job submitted with the ID of %d\n", alternate_jobid);
				dataset_cache_not_execute = 1;
				fprintf(ds_store, "%d\t%d\n", alternate_jobid, desc->dataset_size);
				break;
			}
			else{
				if(i == part_info_ptr->record_count-1) {
					printf("No free partition available\n");
					slurm_kill_job(alternate_jobid, SIGKILL, KILL_JOB_BATCH);
					break;
				}
				slurm_kill_job(alternate_jobid, SIGKILL, KILL_JOB_BATCH);
				alternate_jobid = job_ptr->job_id + 1;
				continue;
			}
		}		*/
	} 
	
	/* the else part gets executed if a job submission was successful */
	else{
		dataset_cache_not_execute = 1;
		changed_partition = fopen("changed_partition", "a");
		int submitted_exeuction_time = 0;	/* when does the submitted job end? */
		if (desc->dataset_size != -2){
			submitted_exeuction_time = 
				calculate_execution_time(desc->dataset_size, job_ptr->name, desc->partition) +
				(int) job_ptr->submit_time;
		}
		//delete_previous_job(desc->partition);
		ds_store = fopen("jobid_dataset", "a");
		fprintf(ds_store, "%s\t%d\t%d\t%d\n", desc->partition, resp->job_id, desc->dataset_size, submitted_exeuction_time);
		printf("Submitted batch job %u\n", resp->job_id);
		/* partition changed? */
		if (strcmp(desc->partition, opt.partition))
			fprintf(changed_partition, "Job ID: %d\t%s -------> %s\n", resp->job_id, opt.partition, desc->partition);
		fclose(changed_partition);
		fclose(ds_store);
	}

	//printf("Start Time: %lld\n", (long long) job_ptr->start_time);
	//printf("Eligible Time: %lld\n", (long long) job_ptr->start_time);
	//printf("End Time: %lld\n", (long long) job_ptr->end_time);

	/*if (dataset_cache_not_execute == 0 && desc->dataset_size > 0)
	{
		FILE *not_executed;
		not_executed = fopen("not_executed", "a");
		fprintf(not_executed, "%s\n", argv[1]);
		char* new_partition;
	 	new_partition = read_from_dataset_file(desc->dataset_size, job_ptr->name);
		//printf("New Partition: %s\n", new_partition);
		desc->partition = new_partition;
		slurm_submit_batch_job(desc, resp);
		alternate_jobid++;
		printf("Job submitted to the queue of %s partition with the shortest exeuction time.\n", new_partition);
	}*/



#ifdef MEMORY_LEAK_DEBUG
	slurm_select_fini();
	slurm_reset_all_options(&opt, false);
	slurm_auth_fini();
	slurm_conf_destroy();
	log_fini();
#endif /* MEMORY_LEAK_DEBUG */
	xfree(script_body);

	return rc;
}

/* Insert the contents of "burst_buffer_file" into "script_body" */
static void  _add_bb_to_script(char **script_body, char *burst_buffer_file)
{
	char *orig_script = *script_body;
	char *new_script, *sep, save_char;
	int i;
	char *bbf = NULL;

	if (!burst_buffer_file || (burst_buffer_file[0] == '\0'))
		return;	/* No burst buffer file or empty file */

	if (!orig_script) {
		*script_body = xstrdup(burst_buffer_file);
		return;
	}
	bbf = xstrdup(burst_buffer_file);
	i = strlen(bbf) - 1;
	if (bbf[i] != '\n')	/* Append new line as needed */
		xstrcat(bbf, "\n");

	if (orig_script[0] != '#') {
		/* Prepend burst buffer file */
		new_script = bbf;
		xstrcat(new_script, orig_script);
		*script_body = new_script;
		return;
	}

	sep = strchr(orig_script, '\n');
	if (sep) {
		save_char = sep[1];
		sep[1] = '\0';
		new_script = xstrdup(orig_script);
		xstrcat(new_script, bbf);
		sep[1] = save_char;
		xstrcat(new_script, sep + 1);
		*script_body = new_script;
		xfree(bbf);
		return;
	} else {
		new_script = xstrdup(orig_script);
		xstrcat(new_script, "\n");
		xstrcat(new_script, bbf);
		*script_body = new_script;
		xfree(bbf);
		return;
	}
}

/* Wait for specified job ID to terminate, return it's exit code */
static int _job_wait(uint32_t job_id)
{
	slurm_job_info_t *job_ptr;
	job_info_msg_t *resp = NULL;
	int ec = 0, ec2, i, rc;
	int sleep_time = 2;
	bool complete = false;

	while (!complete) {
		complete = true;
		sleep(sleep_time);
		/*
		 * min_job_age is factored into this to ensure the job can't
		 * run, complete quickly, and be purged from slurmctld before
		 * we've woken up and queried the job again.
		 */
		if ((sleep_time < (slurm_conf.min_job_age / 2)) &&
		    (sleep_time < MAX_WAIT_SLEEP_TIME))
			sleep_time *= 4;

		rc = slurm_load_job(&resp, job_id, SHOW_ALL);
		if (rc == SLURM_SUCCESS) {
			for (i = 0, job_ptr = resp->job_array;
			     (i < resp->record_count) && complete;
			     i++, job_ptr++) {
				if (IS_JOB_FINISHED(job_ptr)) {
					if (WIFEXITED(job_ptr->exit_code)) {
						ec2 = WEXITSTATUS(job_ptr->
								  exit_code);
					} else
						ec2 = 1;
					ec = MAX(ec, ec2);
				} else {
					complete = false;
				}
			}
			slurm_free_job_info_msg(resp);
		} else if (rc == ESLURM_INVALID_JOB_ID) {
			error("Job %u no longer found and exit code not found",
			      job_id);
		} else {
			complete = false;
			error("Currently unable to load job state information, retrying: %m");
		}
	}

	return ec;
}


/* Propagate select user environment variables to the job.
 * If ALL is among the specified variables propagate
 * the entire user environment as well.
 */
static void _env_merge_filter(job_desc_msg_t *desc)
{
	extern char **environ;
	int i, len;
	char *save_env[2] = { NULL, NULL }, *tmp, *tok, *last = NULL;

	tmp = xstrdup(opt.export_env);
	tok = find_quote_token(tmp, ",", &last);
	while (tok) {

		if (xstrcasecmp(tok, "ALL") == 0) {
			env_array_merge(&desc->environment,
					(const char **)environ);
			tok = find_quote_token(NULL, ",", &last);
			continue;
		}

		if (strchr(tok, '=')) {
			save_env[0] = tok;
			env_array_merge(&desc->environment,
					(const char **)save_env);
		} else {
			len = strlen(tok);
			for (i = 0; environ[i]; i++) {
				if (xstrncmp(tok, environ[i], len) ||
				    (environ[i][len] != '='))
					continue;
				save_env[0] = environ[i];
				env_array_merge(&desc->environment,
						(const char **)save_env);
				break;
			}
		}
		tok = find_quote_token(NULL, ",", &last);
	}
	xfree(tmp);

	for (i = 0; environ[i]; i++) {
		if (xstrncmp("SLURM_", environ[i], 6))
			continue;
		save_env[0] = environ[i];
		env_array_merge(&desc->environment,
				(const char **)save_env);
	}
}

/* Returns 0 on success, -1 on failure */
/* this function fills the structure by reading from the script file */
static int _fill_job_desc_from_opts(job_desc_msg_t *desc)
{
	int i;
	extern char **environ;
	desc->contiguous = opt.contiguous ? 1 : 0;
	if (opt.core_spec != NO_VAL16)
		desc->core_spec = opt.core_spec;
	desc->features = xstrdup(opt.constraint);
	desc->cluster_features = xstrdup(opt.c_constraint);
	if (opt.job_name)
		desc->name = xstrdup(opt.job_name);
	else
		desc->name = xstrdup("sbatch");
	desc->reservation  = xstrdup(opt.reservation);
	desc->wckey  = xstrdup(opt.wckey);
	desc->req_nodes = xstrdup(opt.nodelist);
	desc->extra = xstrdup(opt.extra);
	desc->exc_nodes = xstrdup(opt.exclude);
	desc->partition = xstrdup(opt.partition);
	desc->profile = opt.profile;
	desc->dataset_name = xstrdup(opt.dataset_name);	/* reads dataset name */
	desc->partition = opt.partition;
	//original_partition = opt.partition;	/* this keeps track of what the original partition was */

	/* change partition if needed */
	/* when a dataset size has been specified */

	if (opt.dataset_size){
		char tmp_part[1];
		desc->dataset_size = opt.dataset_size;	/* set dataset size */
		sprintf(tmp_part, "%.*s", 1, desc->partition);	/* this function gets the first letter of the selected partition (for comparison purposes)*/
		/* I/O intensive application */
		if (desc->dataset_size > 50){
			printf("Your application is I/O intensive, with a data set size of %d GB for your application.\n"
				, desc->dataset_size);
			if (desc->dataset_name != NULL){
				/* if dataset is popular */
				if (check_if_dataset_famous(desc->dataset_name))
				{
					/* search for empty local partition(s) */
					bool idle_or_not = false;
					for (int i = number_of_base_parts + number_of_cache_parts; 
					i < number_of_base_parts + number_of_cache_parts + number_of_local_parts; 
					i++)
					{
						if (!strcmp(parts_status[i][1], "idle")){
							desc->partition = parts_status[i][0];
							idle_or_not = true;
							break;
						}
					}
					if (!idle_or_not){
						/* choose a random local partition */
						srand(time(NULL));
						desc->partition = parts_status[(rand() % (number_of_local_parts)) + number_of_base_parts + number_of_cache_parts];
					}
				}
				else	
					desc->partition = select_part(1);
			}
			else
				desc->partition = select_part(1);
		}
		/* not I/O intensive application */
		else{
			printf("Your application is CPU intensive, with a data set size of %d GB for your application.\n"
				, desc->dataset_size);

			desc->partition = select_part(0);
		}
		
	}
	else
	{
		desc->partition = select_part(0);
		/*char tmp_part[1];
		sprintf(tmp_part, "%.*s", 1, desc->partition);
		if (!strcmp("c", tmp_part))
		{
			// cache partition specified with no dataset size. Change partition
			printf("Cache partition specified with no dataset information. Switching to a base partition...\n");
			desc->partition = "base";
		}
		else if (!strcmp("l", tmp_part))
		{
			// local partition specified with no dataset size. Change partition
			printf("Local SSD partition specified with no dataset information. Switching to a base partition...\n");
			desc->partition = "base";
		}
		else ;*/
	}
	//printf("Selected Partition: %s\n", desc->partition);
	if (opt.licenses)
		desc->licenses = xstrdup(opt.licenses);
	if (opt.nodes_set) {
		desc->min_nodes = opt.min_nodes;
		if (opt.max_nodes)
			desc->max_nodes = opt.max_nodes;
	} else if (opt.ntasks_set && (opt.ntasks == 0))
		desc->min_nodes = 0;
	if (opt.ntasks_per_node)
		desc->ntasks_per_node = opt.ntasks_per_node;
	if (opt.ntasks_per_tres != NO_VAL)
		desc->ntasks_per_tres = opt.ntasks_per_tres;
	else if (opt.ntasks_per_gpu != NO_VAL)
		desc->ntasks_per_tres = opt.ntasks_per_gpu;
	desc->user_id = opt.uid;
	desc->group_id = opt.gid;
	if (opt.dependency)
		desc->dependency = xstrdup(opt.dependency);

	if (sbopt.array_inx)
		desc->array_inx = xstrdup(sbopt.array_inx);
	if (sbopt.batch_features)
		desc->batch_features = xstrdup(sbopt.batch_features);
	if (opt.mem_bind)
		desc->mem_bind       = xstrdup(opt.mem_bind);
	if (opt.mem_bind_type)
		desc->mem_bind_type  = opt.mem_bind_type;
	if (opt.plane_size != NO_VAL)
		desc->plane_size     = opt.plane_size;
	desc->task_dist  = opt.distribution;

	desc->network = xstrdup(opt.network);
	if (opt.nice != NO_VAL)
		desc->nice = NICE_OFFSET + opt.nice;
	if (opt.priority)
		desc->priority = opt.priority;

	desc->mail_type = opt.mail_type;
	if (opt.mail_user)
		desc->mail_user = xstrdup(opt.mail_user);
	if (opt.begin)
		desc->begin_time = opt.begin;
	if (opt.deadline)
		desc->deadline = opt.deadline;
	if (opt.delay_boot != NO_VAL)
		desc->delay_boot = opt.delay_boot;
	if (opt.account)
		desc->account = xstrdup(opt.account);
	if (opt.burst_buffer)
		desc->burst_buffer = opt.burst_buffer;
	if (opt.comment)
		desc->comment = xstrdup(opt.comment);
	if (opt.qos)
		desc->qos = xstrdup(opt.qos);

	if (opt.hold)
		desc->priority     = 0;
	if (opt.reboot)
		desc->reboot = 1;

	/* job constraints */
	if (opt.pn_min_cpus > -1)
		desc->pn_min_cpus = opt.pn_min_cpus;
	if (opt.pn_min_memory != NO_VAL64)
		desc->pn_min_memory = opt.pn_min_memory;
	else if (opt.mem_per_cpu != NO_VAL64)
		desc->pn_min_memory = opt.mem_per_cpu | MEM_PER_CPU;
	if (opt.pn_min_tmp_disk != NO_VAL64)
		desc->pn_min_tmp_disk = opt.pn_min_tmp_disk;
	if (opt.overcommit) {
		desc->min_cpus = MAX(opt.min_nodes, 1);
		desc->overcommit = opt.overcommit;
	} else if (opt.cpus_set)
		desc->min_cpus = opt.ntasks * opt.cpus_per_task;
	else if (opt.nodes_set && (opt.min_nodes == 0))
		desc->min_cpus = 0;
	else
		desc->min_cpus = opt.ntasks;

	if (opt.ntasks_set)
		desc->num_tasks = opt.ntasks;
	if (opt.cpus_set)
		desc->cpus_per_task = opt.cpus_per_task;
	if (opt.ntasks_per_socket > -1)
		desc->ntasks_per_socket = opt.ntasks_per_socket;
	if (opt.ntasks_per_core > -1)
		desc->ntasks_per_core = opt.ntasks_per_core;

	/* node constraints */
	if (opt.sockets_per_node != NO_VAL)
		desc->sockets_per_node = opt.sockets_per_node;
	if (opt.cores_per_socket != NO_VAL)
		desc->cores_per_socket = opt.cores_per_socket;
	if (opt.threads_per_core != NO_VAL)
		desc->threads_per_core = opt.threads_per_core;

	if (opt.no_kill)
		desc->kill_on_node_fail = 0;
	if (opt.time_limit != NO_VAL)
		desc->time_limit = opt.time_limit;
	if (opt.time_min  != NO_VAL)
		desc->time_min = opt.time_min;
	if (opt.shared != NO_VAL16)
		desc->shared = opt.shared;

	desc->wait_all_nodes = sbopt.wait_all_nodes;
	if (opt.warn_flags)
		desc->warn_flags = opt.warn_flags;
	if (opt.warn_signal)
		desc->warn_signal = opt.warn_signal;
	if (opt.warn_time)
		desc->warn_time = opt.warn_time;

	desc->environment = NULL;
	if (sbopt.export_file) {
		desc->environment = env_array_from_file(sbopt.export_file);
		if (desc->environment == NULL)
			exit(1);
	}
	if (opt.export_env == NULL) {
		env_array_merge(&desc->environment, (const char **) environ);
	} else if (!xstrcasecmp(opt.export_env, "ALL")) {
		env_array_merge(&desc->environment, (const char **) environ);
	} else if (!xstrcasecmp(opt.export_env, "NONE")) {
		desc->environment = env_array_create();
		env_array_merge_slurm(&desc->environment,
				      (const char **)environ);
		opt.get_user_env_time = 0;
	} else {
		_env_merge_filter(desc);
		opt.get_user_env_time = 0;
	}
	if (opt.get_user_env_time >= 0) {
		env_array_overwrite(&desc->environment,
				    "SLURM_GET_USER_ENV", "1");
	}

	if ((opt.distribution & SLURM_DIST_STATE_BASE) == SLURM_DIST_ARBITRARY){
		env_array_overwrite_fmt(&desc->environment,
					"SLURM_ARBITRARY_NODELIST",
					"%s", desc->req_nodes);
	}

	desc->env_size = envcount(desc->environment);

	desc->argc     = sbopt.script_argc;
	desc->argv     = xmalloc(sizeof(char *) * sbopt.script_argc);
	for (i = 0; i < sbopt.script_argc; i++)
		desc->argv[i] = xstrdup(sbopt.script_argv[i]);
	desc->std_err  = xstrdup(opt.efname);
	desc->std_in   = xstrdup(opt.ifname);
	desc->std_out  = xstrdup(opt.ofname);
	desc->work_dir = xstrdup(opt.chdir);
	if (sbopt.requeue != NO_VAL)
		desc->requeue = sbopt.requeue;
	if (sbopt.open_mode)
		desc->open_mode = sbopt.open_mode;
	if (opt.acctg_freq)
		desc->acctg_freq = xstrdup(opt.acctg_freq);

	if (opt.spank_job_env_size) {
		desc->spank_job_env_size = opt.spank_job_env_size;
		desc->spank_job_env =
			xmalloc(sizeof(char *) * opt.spank_job_env_size);
		for (i = 0; i < opt.spank_job_env_size; i++)
			desc->spank_job_env[i] = xstrdup(opt.spank_job_env[i]);
	}

	desc->cpu_freq_min = opt.cpu_freq_min;
	desc->cpu_freq_max = opt.cpu_freq_max;
	desc->cpu_freq_gov = opt.cpu_freq_gov;

	if (opt.req_switch >= 0)
		desc->req_switch = opt.req_switch;
	if (opt.wait4switch >= 0)
		desc->wait4switch = opt.wait4switch;

	desc->power_flags = opt.power;
	if (opt.job_flags)
		desc->bitflags = opt.job_flags;
	if (opt.mcs_label)
		desc->mcs_label = xstrdup(opt.mcs_label);

	if (opt.cpus_per_gpu)
		xstrfmtcat(desc->cpus_per_tres, "gpu:%d", opt.cpus_per_gpu);
	if (!opt.tres_bind && ((opt.ntasks_per_tres != NO_VAL) ||
			       (opt.ntasks_per_gpu != NO_VAL))) {
		/* Implicit single GPU binding with ntasks-per-tres/gpu */
		if (opt.ntasks_per_tres != NO_VAL)
			xstrfmtcat(opt.tres_bind, "gpu:single:%d",
				   opt.ntasks_per_tres);
		else
			xstrfmtcat(opt.tres_bind, "gpu:single:%d",
				   opt.ntasks_per_gpu);
	}
	desc->tres_bind = xstrdup(opt.tres_bind);
	desc->tres_freq = xstrdup(opt.tres_freq);
	xfmt_tres(&desc->tres_per_job,    "gpu", opt.gpus);
	xfmt_tres(&desc->tres_per_node,   "gpu", opt.gpus_per_node);
	if (opt.gres) {
		if (desc->tres_per_node)
			xstrfmtcat(desc->tres_per_node, ",%s", opt.gres);
		else
			desc->tres_per_node = xstrdup(opt.gres);
	}
	xfmt_tres(&desc->tres_per_socket, "gpu", opt.gpus_per_socket);
	xfmt_tres(&desc->tres_per_task,   "gpu", opt.gpus_per_task);
	if (opt.mem_per_gpu != NO_VAL64)
		xstrfmtcat(desc->mem_per_tres, "gpu:%"PRIu64, opt.mem_per_gpu);

	desc->clusters = xstrdup(opt.clusters);

	return 0;
}

static void _set_exit_code(void)
{
	int i;
	char *val = getenv("SLURM_EXIT_ERROR");

	if (val) {
		i = atoi(val);
		if (i == 0)
			error("SLURM_EXIT_ERROR has zero value");
		else
			error_exit = i;
	}
}

/* Propagate SPANK environment via SLURM_SPANK_ environment variables */
static void _set_spank_env(void)
{
	int i;

	for (i = 0; i < opt.spank_job_env_size; i++) {
		if (setenvfs("SLURM_SPANK_%s", opt.spank_job_env[i]) < 0) {
			error("unable to set %s in environment",
			      opt.spank_job_env[i]);
		}
	}
}

/* Set SLURM_SUBMIT_DIR and SLURM_SUBMIT_HOST environment variables within
 * current state */
static void _set_submit_dir_env(void)
{
	char buf[MAXPATHLEN + 1], host[256];

	if ((getcwd(buf, MAXPATHLEN)) == NULL)
		error("getcwd failed: %m");
	else if (setenvf(NULL, "SLURM_SUBMIT_DIR", "%s", buf) < 0)
		error("unable to set SLURM_SUBMIT_DIR in environment");

	if ((gethostname(host, sizeof(host))))
		error("gethostname_short failed: %m");
	else if (setenvf(NULL, "SLURM_SUBMIT_HOST", "%s", host) < 0)
		error("unable to set SLURM_SUBMIT_HOST in environment");
}

/* Set SLURM_UMASK environment variable with current state */
static int _set_umask_env(void)
{
	char mask_char[5];
	mode_t mask;

	if (getenv("SLURM_UMASK"))	/* use this value */
		return SLURM_SUCCESS;

	if (sbopt.umask >= 0) {
		mask = sbopt.umask;
	} else {
		mask = (int)umask(0);
		umask(mask);
	}

	sprintf(mask_char, "0%d%d%d",
		((mask>>6)&07), ((mask>>3)&07), mask&07);
	if (setenvf(NULL, "SLURM_UMASK", "%s", mask_char) < 0) {
		error ("unable to set SLURM_UMASK in environment");
		return SLURM_ERROR;
	}
	debug ("propagating UMASK=%s", mask_char);
	return SLURM_SUCCESS;
}

/*
 * _set_prio_process_env
 *
 * Set the internal SLURM_PRIO_PROCESS environment variable to support
 * the propagation of the users nice value and the "PropagatePrioProcess"
 * config keyword.
 */
static void  _set_prio_process_env(void)
{
	int retval;

	errno = 0; /* needed to detect a real failure since prio can be -1 */

	if ((retval = getpriority (PRIO_PROCESS, 0)) == -1)  {
		if (errno) {
			error ("getpriority(PRIO_PROCESS): %m");
			return;
		}
	}

	if (setenvf (NULL, "SLURM_PRIO_PROCESS", "%d", retval) < 0) {
		error ("unable to set SLURM_PRIO_PROCESS in environment");
		return;
	}

	debug ("propagating SLURM_PRIO_PROCESS=%d", retval);
}

/*
 * Checks if the buffer starts with a shebang (#!).
 */
static bool has_shebang(const void *buf, int size)
{
	char *str = (char *)buf;

	if (size < 2)
		return false;

	if (str[0] != '#' || str[1] != '!')
		return false;

	return true;
}

/*
 * Checks if the buffer contains a NULL character (\0).
 */
static bool contains_null_char(const void *buf, int size)
{
	char *str = (char *)buf;
	int i;

	for (i = 0; i < size; i++) {
		if (str[i] == '\0')
			return true;
	}

	return false;
}

/*
 * Checks if the buffer contains any DOS linebreak (\r\n).
 */
static bool contains_dos_linebreak(const void *buf, int size)
{
	char *str = (char *)buf;
	char prev_char = '\0';
	int i;

	for (i = 0; i < size; i++) {
		if (prev_char == '\r' && str[i] == '\n')
			return true;
		prev_char = str[i];
	}

	return false;
}

/*
 * If "filename" is NULL, the batch script is read from standard input.
 */
static void *_get_script_buffer(const char *filename, int *size)
{
	int fd;
	char *buf = NULL;
	int buf_size = BUFSIZ;
	int buf_left;
	int script_size = 0;
	char *ptr = NULL;
	int tmp_size;

	/*
	 * First figure out whether we are reading from STDIN_FILENO
	 * or from a file.
	 */
	if (filename == NULL) {
		fd = STDIN_FILENO;
	} else {
		fd = open(filename, O_RDONLY);
		if (fd == -1) {
			error("Unable to open file %s", filename);
			goto fail;
		}
	}

	/*
	 * Then read in the script.
	 */
	buf = ptr = xmalloc(buf_size);
	buf_left = buf_size;
	while((tmp_size = read(fd, ptr, buf_left)) > 0) {
		buf_left -= tmp_size;
		script_size += tmp_size;
		if (buf_left == 0) {
			buf_size += BUFSIZ;
			xrealloc(buf, buf_size);
		}
		ptr = buf + script_size;
		buf_left = buf_size - script_size;
	}
	if (filename)
		close(fd);

	/*
	 * Finally we perform some sanity tests on the script.
	 */
	if (script_size == 0) {
		error("Batch script is empty!");
		goto fail;
	} else if (xstring_is_whitespace(buf)) {
		error("Batch script contains only whitespace!");
		goto fail;
	} else if (!has_shebang(buf, script_size)) {
		error("This does not look like a batch script.  The first");
		error("line must start with #! followed by the path"
		      " to an interpreter.");
		error("For instance: #!/bin/sh");
		goto fail;
	} else if (contains_null_char(buf, script_size)) {
		error("The Slurm controller does not allow scripts that");
		error("contain a NULL character '\\0'.");
		goto fail;
	} else if (contains_dos_linebreak(buf, script_size)) {
		error("Batch script contains DOS line breaks (\\r\\n)");
		error("instead of expected UNIX line breaks (\\n).");
		goto fail;
	}

	*size = script_size;
	return buf;
fail:
	xfree(buf);
	*size = 0;
	return NULL;
}

/* Wrap a single command string in a simple shell script */
static char *_script_wrap(char *command_string)
{
	char *script = NULL;

	xstrcat(script, "#!/bin/sh\n");
	xstrcat(script, "# This script was created by sbatch --wrap.\n\n");
	xstrcat(script, command_string);
	xstrcat(script, "\n");

	return script;
}

/* Set SLURM_RLIMIT_* environment variables with current resource
 * limit values, reset RLIMIT_NOFILE to maximum possible value */
static int _set_rlimit_env(void)
{
	int                  rc = SLURM_SUCCESS;
	struct rlimit        rlim[1];
	unsigned long        cur;
	char                 name[64], *format;
	slurm_rlimits_info_t *rli;

	/* Load default limits to be propagated from slurm.conf */
	slurm_conf_lock();
	slurm_conf_unlock();

	/* Modify limits with any command-line options */
	if (sbopt.propagate && parse_rlimits( sbopt.propagate, PROPAGATE_RLIMITS)){
		error("--propagate=%s is not valid.", sbopt.propagate);
		exit(error_exit);
	}

	for (rli = get_slurm_rlimits_info(); rli->name != NULL; rli++ ) {

		if (rli->propagate_flag != PROPAGATE_RLIMITS)
			continue;

		if (getrlimit (rli->resource, rlim) < 0) {
			error ("getrlimit (RLIMIT_%s): %m", rli->name);
			rc = SLURM_ERROR;
			continue;
		}

		cur = (unsigned long) rlim->rlim_cur;
		snprintf(name, sizeof(name), "SLURM_RLIMIT_%s", rli->name);
		if (sbopt.propagate && rli->propagate_flag == PROPAGATE_RLIMITS)
			/*
			 * Prepend 'U' to indicate user requested propagate
			 */
			format = "U%lu";
		else
			format = "%lu";

		if (setenvf (NULL, name, format, cur) < 0) {
			error ("unable to set %s in environment", name);
			rc = SLURM_ERROR;
			continue;
		}

		debug ("propagating RLIMIT_%s=%lu", rli->name, cur);
	}

	/*
	 *  Now increase NOFILE to the max available for this srun
	 */
	rlimits_maximize_nofile();

	return rc;
}

extern int
scontrol_load_partitions (partition_info_msg_t **part_buffer_pptr)
{
	int error_code;
	static uint16_t last_show_flags = 0xffff;
	uint16_t show_flags = 0;

	if (all_flag)
		show_flags |= SHOW_ALL;

	if (old_part_info_ptr) {
		if (last_show_flags != show_flags)
			old_part_info_ptr->last_update = (time_t) 0;
		error_code = slurm_load_partitions (
			old_part_info_ptr->last_update,
			&part_info_ptr, show_flags);
		if (error_code == SLURM_SUCCESS)
			slurm_free_partition_info_msg (old_part_info_ptr);
		else if (slurm_get_errno () == SLURM_NO_CHANGE_IN_DATA) {
			part_info_ptr = old_part_info_ptr;
			error_code = SLURM_SUCCESS;
			if (quiet_flag == -1)
				printf ("slurm_load_part no change in data\n");
		}
	} else {
		error_code = slurm_load_partitions((time_t) NULL,
						   &part_info_ptr, show_flags);
	}

	if (error_code == SLURM_SUCCESS) {
		old_part_info_ptr = part_info_ptr;
		last_show_flags = show_flags;
		*part_buffer_pptr = part_info_ptr;
	}

	return error_code;
}

extern void
scontrol_print_part (char *partition_name)
{
	int error_code, i, print_cnt = 0;
	partition_info_msg_t *part_info_ptr = NULL;

	error_code = scontrol_load_partitions(&part_info_ptr);
	if (error_code) {
		exit_code = 1;
		if (quiet_flag != 1)
			slurm_perror ("slurm_load_partitions error");
		return;
	}

	if (quiet_flag == -1) {
		char time_str[32];
		slurm_make_time_str ((time_t *)&part_info_ptr->last_update,
			       time_str, sizeof(time_str));
		printf ("last_update_time=%s, records=%d\n",
			time_str, part_info_ptr->record_count);
	}

	part_ptr = part_info_ptr->partition_array;
	for (i = 0; i < part_info_ptr->record_count; i++) {
		if (partition_name &&
		    xstrcmp (partition_name, part_ptr[i].name) != 0)
			continue;
		print_cnt++;
		//printf("Partition Name: %s\nPartition State: %d\n", part_ptr[i].name, part_ptr[i].state_up);
		//slurm_print_partition_info (stdout, & part_ptr[i],
		//                            one_liner ) ;
		if (partition_name)
			break;
	}

	if (print_cnt == 0) {
		if (partition_name) {
			exit_code = 1;
			if (quiet_flag != 1)
				printf ("Partition %s not found\n",
				        partition_name);
		} else if (quiet_flag != 1)
			printf ("No partitions in the system\n");
	}
}


/* added by Sinashan */ 
/* this function is not called as of now */
extern 
char *read_from_dataset_file(int current_dataset, char *job_name)
{
	FILE *fp;
    char *line = NULL;
	char *jobid_token;
	char *dataset_token;
	char *cache_part_name;
	char *base_part_name = "base";
	char *app_name;	/* taken from job name */
    size_t len = 0;
    ssize_t read;
	int dataset_size, hdd_exec_time, ssd_exec_time;
	uint32_t execution, finish_time, time_min_cache, time_min_base, base_execution, avg_io_rate;
	double cache_hit, current_execution_time;

	/* the new ones */
	double time_on_cache, total_time_on_cache,
			normalized_io_time_on_cache, compute_time,
			avg_io_rate_on_cache, io_time_on_hdd, total_time_on_hdd;

	slurm_job_info_t *job_ptr;	
	job_info_msg_t *resps = NULL;
	
    fp = fopen("jobid_dataset", "r");
    if (fp == NULL)
        exit(EXIT_FAILURE);

	time_min_cache = ((uint32_t) time(NULL)) * 2;
	time_min_base = ((uint32_t) time(NULL)) * 2;


	app_name = strtok(job_name, "_");
	//printf("%s\n", app_name);
	if (current_dataset <= 500)
	{
		cache_hit = 1;
	}
	else if (!strcmp(app_name, "tensorflow"))
	{
		cache_hit = tensorflow_hit;
		total_time_on_cache = (current_dataset / tensorflow_dataset_size) * tensorflow_average_exec_time;
		normalized_io_time_on_cache = total_time_on_cache * tensorflow_normalized_io;
		compute_time = total_time_on_cache - tensorflow_normalized_io;
		avg_io_rate_on_cache = (tensorflow_avg_read_hit * SSD_BW) + ((1 - tensorflow_avg_read_hit) * HDD_BW);
		io_time_on_hdd = (avg_io_rate_on_cache / HDD_BW) * normalized_io_time_on_cache; 
		total_time_on_hdd = compute_time + io_time_on_hdd;
	}
	else if (!strcmp(app_name, "pytorch"))
	{
		cache_hit = pytorch_hit;
		total_time_on_cache = (current_dataset / pytorch_dataset_size) * pytorch_average_exec_time;
		normalized_io_time_on_cache = total_time_on_cache * pytorch_normalized_io;
		compute_time = total_time_on_cache - pytorch_normalized_io;
		avg_io_rate_on_cache = (pytorch_avg_read_hit * SSD_BW) + ((1 - pytorch_avg_read_hit) * HDD_BW);
		io_time_on_hdd = (avg_io_rate_on_cache / HDD_BW) * normalized_io_time_on_cache; 
		total_time_on_hdd = compute_time + io_time_on_hdd;
	}
	else if (!strcmp(app_name, "opencv"))
	{
		cache_hit = opencv_hit;
		total_time_on_cache = (current_dataset / opencv_dataset_size) * opencv_average_exec_time;
		normalized_io_time_on_cache = total_time_on_cache * opencv_normalized_io;
		compute_time = total_time_on_cache - opencv_normalized_io;
		avg_io_rate_on_cache = (opencv_avg_read_hit * SSD_BW) + ((1 - opencv_avg_read_hit) * HDD_BW);
		io_time_on_hdd = (avg_io_rate_on_cache / HDD_BW) * normalized_io_time_on_cache; 
		total_time_on_hdd = compute_time + io_time_on_hdd;
	}
	else if (!strcmp(app_name, "python"))
	{
		cache_hit = python_hit;
		total_time_on_cache = (current_dataset / python_dataset_size) * python_average_exec_time;
		normalized_io_time_on_cache = total_time_on_cache * python_normalized_io;
		compute_time = total_time_on_cache - python_normalized_io;
		avg_io_rate_on_cache = (python_avg_read_hit * SSD_BW) + ((1 - python_avg_read_hit) * HDD_BW);
		io_time_on_hdd = (avg_io_rate_on_cache / HDD_BW) * normalized_io_time_on_cache; 
		total_time_on_hdd = compute_time + io_time_on_hdd;
	}

	/* it should return from here */
	

	/* execution time on base partition */
	hdd_exec_time = (1 - cache_hit) * EXECUTION_TIME;
	//printf("HDD Time: %d\n", hdd_exec_time);
	ssd_exec_time = EXECUTION_TIME - hdd_exec_time;
	//printf("SSD Time: %d\n", ssd_exec_time);
	ssd_exec_time *= (SSD_BW / HDD_BW);
	//printf("SSD Time if on HDD: %d\n", ssd_exec_time);
	//ssd_exec_time *= (SSD_IOPS / HDD_IOPS);
	hdd_exec_time += ssd_exec_time;
	//printf("All HDD Time: %d\n", hdd_exec_time);
	base_execution = (uint32_t) time(NULL) + (uint32_t) hdd_exec_time;
	printf("Execution on a base (no cache) partition: %d\tFinish Time: %d\n", \
				hdd_exec_time, base_execution);
	
	current_execution_time = ((current_dataset * 1024 * cache_hit) / SSD_BW) + \
								((current_dataset * 1024 * (1-cache_hit)) / HDD_BW);
	printf("Current Exeuction time considering cache hits: %d\n", (uint32_t) current_execution_time);
	return;

	/* DO NOT EXECUTE THIS */
    while ((read = getline(&line, &len, fp)) != -1) {
		jobid_token = strtok(line, "\t");	/* Job ID */
		dataset_token = strtok(NULL, "\t");	/* dataset */
		dataset_size = atoi(dataset_token);
		char *previous_job_name;
		char *previous_app_name;
		double previous_cache_hit;
		uint32_t previous_exec_time;
		int partition_counter = 0;
		char *base = "base";
		char *base_num;

		/* cache partition */
		if (dataset_size > 0)
		{
			slurm_load_job(&resps, (uint32_t) atoi(jobid_token), 0);
			job_ptr = resps->job_array;
			previous_job_name = job_ptr->name;
			previous_app_name = strtok(job_name, "_");
			if (dataset_size <= 500)
				previous_cache_hit = 1;
			else if (!strcmp(previous_app_name, "tensorflow"))
				previous_cache_hit = tensorflow_hit;
			else if (!strcmp(previous_app_name, "pytorch"))
				previous_cache_hit = pytorch_hit;
			else if (!strcmp(previous_app_name, "opencv"))
				previous_cache_hit = opencv_hit;
			else if (!strcmp(previous_app_name, "python"))
				previous_cache_hit = python_hit;
			else
				previous_cache_hit = 0.1;

			previous_exec_time = ((previous_cache_hit * dataset_size * 1024) / SSD_BW) \
								+ (((1- previous_cache_hit) * dataset_size * 1024) / HDD_BW);
			//execution = ((dataset_size * EXECUTION_TIME) / CACHE_PART_SIZE) * 60;	/* to seconds */
			finish_time = (uint32_t) job_ptr->start_time + previous_exec_time;
			if (finish_time < time_min_cache)
			{
				time_min_cache = finish_time;
				cache_part_name = job_ptr->partition;
			}
		
		}
	
		/* base partition */ 
		else
		{
			partition_counter++;
			sprintf(base_num,"%d", partition_counter);
			base_part_name = strcat(base_part_name, base_num);
		}
		
	}

	printf("%s\n", base_part_name);

    fclose(fp);
    if (line)
        free(line);

	printf("Cache Execution: %d\n", time_min_cache + (uint32_t) current_execution_time);

	if (base_execution < time_min_cache + (uint32_t) current_execution_time) return;
	else return cache_part_name;
}

/* iterates insie the list of famous datasets to see if the dataset name user sepcified
in the script belongs to one of the list */
extern
int check_if_dataset_famous(char* ds_name){
	int famous = 0;
	for (int i = 0; i < sizeof(datasets) / sizeof(datasets[0]); i++){
		if (!strcmp(datasets[i], ds_name)){
			famous = 1;
		}
	}
	return famous;
}

/* 0: random, 1: sequential */
extern
int check_app_name_io_type(char* job_name){
	//printf("App name: %s\n", job_name);
	char *app_name;
	app_name = strtok(job_name, "_");
	if (!strcmp(app_name, "tensorflow"))
		return tensorflow_io_type;
	else if(!strcmp(app_name, "pytorch"))
		return pytorch_io_type;
	else if(!strcmp(app_name, "opencv"))
		return opencv_io_type;
	else if(!strcmp(app_name, "python"))
		return python_io_type;
	else if(!strcmp(app_name, "fioseq"))
	{
		return fio_seq_io_type;
	}
	else if(!strcmp(app_name, "fiozipf"))
	{
		return fio_zipf_rand_io_type;
	}
	else if(!strcmp(app_name, "fio"))
	{
		return fio_rand_io_type;
	}
	/*else if (!strcmp(app_name, "fio"))
	{
		if (fio_count == 0){
			fio_test = strtok(NULL, "_");
			fio_count++;
			if (!strcmp(fio_test, "seq"))
				return fio_seq_io_type;
			else
				return fio_zipf_rand_io_type;

		}
		else{
			fio_count = 0;
			if (!strcmp(fio_test, "seq"))
				return fio_seq_io_type;
			else
				return fio_zipf_rand_io_type;

		}
	}*/
	else
		return 0;
}

/* 0: smallert than threshold, 1: larger than threshold*/
extern
int check_app_hit_threshold(char* job_name){
	//printf("App hit: %s\n", job_name);
	char *app_name;
	double hit_threshold = 0.8;
	app_name = strtok(job_name, "_");

	if (!strcmp(app_name, "tensorflow"))
	{
		if (tensorflow_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	}
	else if(!strcmp(app_name, "pytorch"))
	{
		if (pytorch_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	}
	else if(!strcmp(app_name, "opencv"))
	{
		if (opencv_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	
	}
	else if(!strcmp(app_name, "python"))
	{
		if (python_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	}
	else if(!strcmp(app_name, "fioseq"))
	{
		if (fio_seq_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	}
	else if(!strcmp(app_name, "fiozipf"))
	{
		if (fio_zipf_rand_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	}
	else if(!strcmp(app_name, "fio"))
	{
		if (fio_rand_avg_read_hit > hit_threshold)
			return 1;
		else
			return 0;
	}
	
	return 1;
}


/* called by earliest_finish_time() to calculate exeuction time on each partition */
extern
int calculate_execution_time(int dataset, char* job, char* partition){
	//printf("Exec Time: %s\n", job);
	char* app_name;
	double hit = 0.5;
	app_name = strtok(job, "_");
	int rand_or_seq = check_app_name_io_type(job); // 0: rand, 1: seq

	if (!strcmp(app_name, "tensorflow"))
	{
		hit = tensorflow_avg_read_hit;
	}
	else if(!strcmp(app_name, "pytorch"))
	{
		hit = pytorch_avg_read_hit;
	}
	else if(!strcmp(app_name, "opencv"))
	{
		hit = opencv_avg_read_hit;
	}
	else if(!strcmp(app_name, "python"))
	{
		hit = python_avg_read_hit;
	}
	else if(!strcmp(app_name, "fioseq"))
	{
		hit = fio_seq_avg_read_hit;
	}
	else if(!strcmp(app_name, "fiozipf"))
	{

		hit = fio_zipf_rand_avg_read_hit;
	}
	else if(!strcmp(app_name, "fio"))
	{
		hit = fio_rand_avg_read_hit;
	}

	if (!strcmp(partition, "base"))
	{
		// random
		if (rand_or_seq == 0)
			return (int) (((dataset * 1024 * 1024) / HDD_BLK_SIZE) / HDD_IOPS);
		// sequential
		else;
			return (int) ((dataset * 1024) / HDD_BW);
	}
	else if (!strcmp(partition, "cache"))
	{
		// random
		if (rand_or_seq == 0)
			return (int) (((hit * dataset * 1024 * 1024) / SSD_BLK_SIZE) / SSD_IOPS);
		// sequential
		else
			return (int) ((hit * dataset * 1024) / SSD_BW);
	}
	else if (!strcmp(partition, "local"))
	{
		// random
		if (rand_or_seq == 0)
			return (int) (((dataset * 1024 * 1024) / SSD_BLK_SIZE) / SSD_IOPS);
		// sequential
		else
			return (int) ((dataset * 1024) / SSD_BW);
	}

}


/* deletes previous job on a specific partition so that each partition has only one row */
extern 
void delete_previous_job(char* partition){
	FILE *ds_store;
	FILE *tmp;
	char * line = NULL;
    size_t len = 0;
    ssize_t read;
	int line_count = 0; // counts which line should be deleted
	ds_store = fopen("jobid_dataset", "r");

	if (ds_store == NULL)
        return;
	
	tmp = fopen("temp", "w");
    while ((read = getline(&line, &len, ds_store)) != -1) {
        if (strstr(line, partition) == NULL){
			fprintf(tmp, "%s", line);
		}
    }

	remove("jobid_dataset");
	rename("temp", "jobid_dataset");

	fclose(tmp);
	fclose(ds_store);
}

/* selectes the partition with the earliest finish time (according to the flowchart) */
extern 
char* earliest_finish_time(int dataset, char* job){
	FILE *ds_store;
	char * line = NULL;
	size_t len = 0;
	ssize_t read;
	int finish_time;
	int hich1, hich2;
	char* early_partition;
	char* selected_partition;
	ds_store = fopen("jobid_dataset", "r");
	int current_job_time;


	if (ds_store == NULL)
        return;
	
	uint32_t min_finish_time =  ((uint32_t) time(NULL)) + 604800;
	int count = 0;
	int base_exec_time, cache_exec_time, local_exec_time;
	base_exec_time = calculate_execution_time(dataset, job, "base");
	cache_exec_time = calculate_execution_time(dataset, job, "cache");
	local_exec_time = calculate_execution_time(dataset, job, "local");
	//printf("Base: %d\n", base_exec_time);
	//printf("Cache: %d\n", cache_exec_time);
	//printf("Local: %d\n", local_exec_time);
	while (read = getline(&line, &len, ds_store) != -1){
		line = strtok(line, "\t");
		early_partition = line;
		while (line != NULL){
			count++;
			line = strtok(NULL, "\t");
			if (count == 3){
				current_job_time = atoi(line);

				if (!strcmp(early_partition, "base"))
					current_job_time = current_job_time + base_exec_time;
				else if (!strcmp(early_partition, "cache"))
					current_job_time = current_job_time + cache_exec_time;
				else if (!strcmp(early_partition, "local"))
					current_job_time = current_job_time + local_exec_time;

				if (min_finish_time > current_job_time){
					min_finish_time = current_job_time;
					selected_partition = early_partition;
				}
				count = -1;
			}
		}
	}
	fclose(ds_store);
	return selected_partition;
}


/* false: partition empty, true: partition busy */
extern
char* select_part(int io_intensive){
	char* selected_partition = "NULL";
	check_parts_status();	/* get the most recent status of partitions */

	if (io_intensive){
		/* search cache partition(s) */
		for (int i = number_of_base_parts; i <= number_of_base_parts + number_of_cache_parts; i++)
		{
			if (!strcmp(parts_status[i][1], "idle"))
				selected_partition = parts_status[i][0];
		}
		if (!strcmp(selected_partition, "NULL")){
			/* search base partition(s)*/
			for (int i = 0; i <= number_of_base_parts; i++){
				if (!strcmp(parts_status[i][1], "idle"))
					selected_partition = parts_status[i][0];
			}

			/* no free base partition, choose one randomly */
			if (!strcmp(selected_partition, "NULL")){
				// Set the seed for the random number generator
				srand(time(NULL));

				// Generate a random number within the specified range
				selected_partition = parts_status[(rand() % (number_of_base_parts))];
			}
		}

		/*if (!strcmp(parts_status[number_of_base_parts][1], "idle"))
				selected_partition = "cache";
		else{
			for (int i = 0; i <= number_of_base_parts; i++){
					if (!strcmp(parts_status[i][1], "idle"))
						selected_partition = parts_status[i][0];
			}
		}
		if (!strcmp(selected_partition, "NULL")){
			if (!strcmp(parts_status[number_of_base_parts][1], "idle"))
				selected_partition = "cache";
			else
				selected_partition = "base";
		}*/
	}
	else if (!io_intensive){
		/* search base partition(s)*/
		for (int i = 0; i < number_of_base_parts; i++){
			if (!strcmp(parts_status[i][1], "idle"))
			{
				selected_partition = parts_status[i][0];
				break;
			}
		}
		if (!strcmp(selected_partition, "NULL")){
			/* search cache partition(s)*/
			for (int i = number_of_base_parts; i < number_of_base_parts + number_of_cache_parts; i++){
				if (!strcmp(parts_status[i][1], "idle"))
				{
					selected_partition = parts_status[i][0];
					break;
				}
			}

			/* no free base partition, choose one randomly */
			if (!strcmp(selected_partition, "NULL")){
				// Set the seed for the random number generator
				srand(time(NULL));

				// Generate a random number within the specified range
				selected_partition = parts_status[(rand() % (number_of_base_parts))];
			}
		}
	}

	return selected_partition;
}

extern
char* check_parts_status(){
	FILE *fp;
    char command[] = "sinfo | awk 'NR>1 {print $1, $5}'";
    char line[256];
    int row = 0, col = 0;

    fp = popen(command, "r");
    if (fp == NULL) {
        printf("Failed to execute command\n");
        return 1;
    }

    while (fgets(line, 256, fp) != NULL) {
        char *token = strtok(line, " \t\n");
        while (token != NULL) {
            char *star = strchr(token, '*');
            if (star != NULL) {
                *star = '\0';
            }
            strcpy(parts_status[row][col], token);
            token = strtok(NULL, " \t\n");
            col++;
        }
        col = 0;
        row++;
	}

    pclose(fp);
}
