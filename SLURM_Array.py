#!/usr/bin/env python2.7

# Written by Shawn O'Neil, CGRB, OSU, Jan 2014
#This software is not free.

#This software program and documentation are copyrighted by 
#Oregon State University. The software program and 
#documentation are supplied "as is", without any accompanying 
#services from Oregon State University. OSU does not warrant 
#that the operation of the program will be uninterrupted or 
#error-free. The end-user understands that the program was 
#developed for research purposes and is advised not to rely 
#exclusively on the program for any reason.

#IN NO EVENT SHALL OREGON STATE UNIVERSITY BE LIABLE TO ANY 
#PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR 
#CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF 
#THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF 
#OREGON STATE UNIVERSITYHAS BEEN ADVISED OF THE POSSIBILITY OF 
#SUCH DAMAGE. OREGON STATE UNIVERSITY SPECIFICALLY DISCLAIMS 
#ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
#PURPOSE AND ANY STATUTORY WARRANTY OF NON-INFRINGEMENT. THE 
#SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND 
#OREGON STATE UNIVERSITY HAS NO OBLIGATIONS TO PROVIDE 
#MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR 
#MODIFICATIONS.


import sys
import io
import re
import math
import argparse
import datetime
from datetime import timedelta
import os
import subprocess
import shutil
import time
import textwrap

### Input parsing. Returns an environment with members like args.queue, args.commands, args.filelimit, etc. 
def parse_input():
	
	parser = argparse.ArgumentParser(description='Runs a list of commands specified on stdin as a SLURM array job. \nExample usage: cat `commands.txt | SLURM_Array` or `SLURM_Array -c commands.txt`')
	parser.add_argument('-c', '--commandsfile', required = False, dest = "commandsfile", default = "-", help = "The file to read commands from. Default: -, meaning standard input.")
	parser.add_argument('-q', '--queue', required = False, dest = "queue", help = "The queue(s) to send the commands to. Default: all queues you have access to.")
	parser.add_argument('-m', '--memory', required = False, dest = "memory", default = "4gb", help = "Amount of free RAM to request for each command, and the maximum that each can use without being killed. Default: 4gb")
	parser.add_argument('-t', '--time', required = False, dest = "time", type = str, default = "04:00:00", help = "The maximum amount of time for the job to run in d-hh:mm:ss. Default: 04:00:00")
	parser.add_argument('-l', '--module', required = False, dest = "module", default = "", type = str, nargs = "+", help = "List of modules to load after preamble. Eg: R/3.3 python/3.6")
	parser.add_argument('-M', '--mail', required = False, dest = "mail", type = str, help = "Email address to send notifications to. Default: None")
	parser.add_argument('--mailtype', required = False, dest = "mailtype", default = "ALL", type = str, help = "Type of email notification to be sent if -M is specified. Options: BEGIN, END, FAIL, ALL. Default: ALL")
	parser.add_argument('-f', '--filelimit', required = False, dest = "filelimit", default = "500G", help = "The largest file a command can create without being killed. (Preserves fileservers.) Default: 500G")
	parser.add_argument('-b', '--concurrency', required = False, dest = "concurrency", default = "1000", help = "Maximum number of commands that can be run simultaneously across any number of machines. (Preserves network resources.) Default: 1000")
	parser.add_argument('-x', '--maxcommands', required = False, dest = "maxcommands", default = 900, type = int, help = "Maximum number of commands that can be submitted with one submission script. If the number of commands exceeds this number, they will be batched in separate array jobs. Default: 900")
	parser.add_argument('--duration', required = False, dest = "duration", default = "24:00:00", type = str, help = "Duration expected for each of maxcommands to run in d-hh:mm:ss. This will be multiplied by the number of batches needed to run.")
	parser.add_argument('-P', '--processors', required = False, dest = "processors", default = "1", help = "Number of processors to reserve for each command. Default: 1")
	parser.add_argument('-r', '--rundir', required = False, dest = "rundir", help = "Job name and the directory to create or OVERWRITE to store log information and standard output of the commands. Default: 'jYEAR-MON-DAY_HOUR-MIN-SEC_<cmd>_etal' where <cmd> is the first word of the first command.")
	parser.add_argument('-w', '--working-directory', required = False, dest = "wd", type = str, help = "Working directory to set. Defaults to nothing.")
	parser.add_argument('-H', required = False, action = 'store_true', dest = "HOLD", help = "Hold the execution for these commands until you release them via scontrol release <JOB-ID>")
	parser.add_argument('--hold', required = False, action = 'store_true', dest = "hold", help = "Hold the execution for these commands until all previous jobs arrays run from this directory have finished. Uses the list of jobs as logged to .slurm_array_jobnums.")
	parser.add_argument('--hold_jids', required = False, dest = "hold_jid_list", help = "Hold the execution for these commands until these specific job IDs have finished (e.g. '--hold_jid 151235' or '--hold_jid 151235,151239' )")
	parser.add_argument('--hold_names', required = False, dest = "hold_name_list", help = "Hold the execution for these commands until these specific job names have finished (comma-sep list); accepts regular expressions. (e.g. 'SLURM_Array -c commands.txt -r this_job_name --hold_names previous_job_name,other_jobs_.+'). Uses job information as logged to .slurm_array_jobnums.")
	parser.add_argument('-v', '--version', action = 'version', version = '%(prog)s 1.0.2.z.99')
	parser.add_argument('-d', '--debug', action = 'store_true', dest = "debug", help = "Create the directory and script, but do not submit")
	parser.add_argument('--showchangelog', required = False, action = 'store_true', dest = "showchangelog", help = "Show the changelog for this program.")

	changelog = textwrap.dedent('''\
		Version 1.0.2.z.99: Fixed bug where multiple tasks were running for each command when maxcommands was exceeded.
		Version 1.0.1.z.99: Add working directory argument and comments to maxcommands jobs
		Version 1.0.0.z.99: This version now has maxcommands and duration for running 1000s of jobs on the cluster. I've also changed the behavior so that the output files index from zero. It's a big change, so that's why I'm incrementing it.
		Version 0.11.1.z.99: Update concurrency to 1000
		Version 0.11.0.z.99: Revert behavior to write.slurm_array_jobnums to current working directory.
		Version 0.10.0.z.99: Add the -H command to act as -H on SLURM clusters
		Version 0.9.2.z.99: Concurrency default set to 2000 based on speaking with HCC people.
		Version 0.9.1.z.99: Changed behavior back so that rundir is created relative to the current working directory.
		Version 0.9.0.z.99: Added new options '-t', '-d', and '-w' to set the time, a debugging flag, and working directory.
		Version 0.8.1.z.99: Changed behavior so .slurm_array_jobnums is written to the $WORK directory.
		Version 0.8.0.z.99: Added new options '-M' and '--mailtype' to email the user I also changed module flag to '-l' for "load module"
		Version 0.7.0.z.99: Zhian Kamvar's translation to SLURM. Currently still a work in progress, but has basic functionality.
		Version 0.6.8.1: Fixed bug so that -r option strips trailing slashes properly; e.g. -r log_dir/ now works properly
		Version 0.6.8: --hold_names option now accepts regular expressions for holding against sets of jobs easily. Eg. --hold_names assembly_.+
		Version 0.6.7.1: Fixed the -r option to now accept paths. e.g SGE_Array -c commands.txt -r logs_dir/log_dir. The "name" of the job (for --hold_names purposes) is logs_dir/log_dir; the SGE name is just log_dir.
		Version 0.6.7: Added new option --hold_names for holding for specific job names.
		Version 0.6.6: Added new option --hold_jid for holding for specific job ids (in addition to --hold which holds for all jobs previously run in the current dir.)
		Version 0.6.5: Fixed some bugs, also, new option --hold
		Version 0.6: Initial version. Reads commands on stdin or from a file, runs them as an array job.
		''')

	## Parse the arguments
	args = parser.parse_args()
	
	if args.showchangelog:
		print(changelog)
		quit()

	## Read the commands on standard input, showing an error if there is no stdin
	cmds = None
	if args.commandsfile == "-":
		if sys.stdin.isatty():
			print(parser.format_help())
			quit()
		cmds = sys.stdin.read().strip().split('\n')
	else:
		cmdsh = io.open(args.commandsfile, "rb")
		cmds = cmdsh.read().strip().split('\n')
	args.commands = cmds

	## grab the executable of the first word of the first command
	cmd = re.split(r"\s+", cmds[0])[0]
	cmd = os.path.basename(cmd)
	cmd = re.subn(r"[^A-Za-z0-9]", "", cmd)[0]
	args.timestamp = datetime.datetime.now().strftime("j%Y-%m-%d_%H-%M-%S_" + cmd + "_etal")
	## Set the rundir and path if not already set
	if args.rundir == None:
		rundir = args.timestamp
		args.rundir = rundir

	args.rundir = re.subn(r"/$", "", args.rundir)[0]

	return args

def get_nruns(args):
	cmds    = len(args.commands)
	maxcmds = args.maxcommands
	NRUNS   = int(math.ceil(cmds/float(maxcmds)))
	return NRUNS

def too_many_commands(args):
	return len(args.commands) > args.maxcommands

def get_duration(the_time):
	# Acceptable time formats include 
	# "minutes", 
	# "minutes:seconds", 
	# "hours:minutes:seconds", 
	# "days-hours", 
	# "days-hours:minutes" and 
	# "days-hours:minutes:seconds".
	jobtime = the_time.split('-')
	days    = 0
	hours   = 0
	minutes = 0
	seconds = 0
	if len(jobtime) == 2:
		days    = int(jobtime[0])
		jobtime = jobtime[1]
	else:
		jobtime = jobtime[0]
	jobtime = jobtime.split(":")
	if len(jobtime) == 1:
		if days is not 0:
			minutes = jobtime[0]
		else:
			hours = jobtime[0]
	elif len(jobtime) == 2:
		if days is not 0:
			hours   = jobtime[0]
			minutes = jobtime[1]
		else:
			minutes = jobtime[0]
			seconds = jobtime[1]
	else:
		hours    = jobtime[0]
		minutes  = jobtime[1]
		seconds  = jobtime[2]
	duration = timedelta(days = int(days), hours = int(hours), minutes = int(minutes), seconds = int(seconds))
	return duration

def get_new_duration(args):
	NRUNS = get_nruns(args)
	duration = get_duration(args.duration) * NRUNS
	return "-".join(str(duration).split(" days, "))

def get_hold_jobs():
	jobslist = list()
	if os.path.isfile(SAJ):
		fhandle = io.open(SAJ, "rb")
		for line in fhandle:
			line_list = line.strip().split('\t')
			jobnum = line_list[0].split('.')[0]
			jobslist.append(jobnum)
		fhandle.close()
	
	return jobslist

## given a comma-sep list of job names, returns a python list of job numbers
def get_hold_jobs_by_names(names):
	jobslist = list()
	job_names_to_nums = dict()
	if os.path.isfile(SAJ):
		fhandle = io.open(SAJ, "rb")
		for line in fhandle:
			line_list = line.strip().split('\t')
			jobnum = line_list[0].split('.')[0]
			jobname = line_list[2]
			job_names_to_nums[jobname] = jobnum
		fhandle.close()
	
	names_list = names.split(',')
	for name in names_list:
		found = False
		for prev_name in job_names_to_nums.keys():
			if re.search(name, prev_name):
				jobslist.append(job_names_to_nums[prev_name])
				found = True
				
		if not found:
			sys.stderr.write("Warning: job " + name + " does not match any job name in " + SAJ + "; cannot hold for this job.\n")

		## Previous: before using regex matching
		#if job_names_to_nums.has_key(name):
		#	jobslist.append(job_names_to_nums[name])
		#else:
		#	sys.stderr.write("Warning: job " + name + " is not a recognized job name in .slurm_array_jobnums; cannot hold for this job.\n")

	return jobslist

########## make dir
def make_rundir(rundir):
	if not os.path.exists(rundir):
		os.makedirs(rundir)
	else:
		print("WARNING: deleting logdir '" + rundir + "' and recreating it in:")
		for i in [" 3..", " 2..", " 1.."]:
			print(i)
			time.sleep(2)
		shutil.rmtree(rundir)
		os.makedirs(rundir)


########## write commands.txt
def write_commands(cmds, rundir):
	commandsh = io.open(rundir + "/commands.txt", "wb")
	for cmd in cmds:
		commandsh.write(cmd + "\n")
	commandsh.close()



########## write the qsub script to args.rundir/args.rundir.sh
def write_qsub(args):
	jobname    = os.path.basename(args.rundir)
	scripth    = io.open(args.rundir + "/" + jobname + ".sh", "wb")
	NRUNS      = get_nruns(args)


	scripth.write(textwrap.dedent('''\
		#!/usr/bin/env bash
		#
		# This file created by SLURM_Array
		#
		# \n'''))
	scripth.write("# Set job name \n")
	scripth.write("#SBATCH --job-name=" + str(jobname) + "\n")
	scripth.write("# \n")

	scripth.write("# Set job time \n")
	if not too_many_commands(args):
		scripth.write("#SBATCH --time=" + args.time + "\n")
		scripth.write("# \n")
		scripth.write("# Set array job range (0 to number of commands in cmd file (minus 1)) and concurrency (%N) \n")
		scripth.write("#SBATCH --array=0-" + str(len(args.commands) - 1) + "%" + str(args.concurrency) + "\n")
		scripth.write("# \n")
	else:
		scripth.write("#SBATCH --time=" + get_new_duration(args) + "\n")
		scripth.write("# \n")
		scripth.write("# Set array job range (0 to number of commands in cmd file (minus 1)) and concurrency (%N) \n")
		scripth.write("#SBATCH --array=0-" + str(args.maxcommands - 1) + "\n")
		scripth.write("# \n")

	scripth.write("# Output files for stdout and stderr \n")
	scripth.write("#SBATCH --output=" + args.rundir + "/" + jobname + ".%A_%a.out\n")
	scripth.write("#SBATCH --error=" + args.rundir + "/" + jobname + ".%A_%a.err\n")
	scripth.write("# \n")

	if args.queue != None:
		scripth.write("# Set partitions to use \n")
		scripth.write("#SBATCH --partition=" + str(args.queue) + "\n")
		scripth.write("# \n")

	# if holding...
	if args.hold or args.hold_jid_list != None or args.hold_name_list != None:
		holdfor = list()
		if args.hold_name_list != None:
			prev_jobs = get_hold_jobs_by_names(args.hold_name_list)
			holdfor.extend(prev_jobs)			
		if args.hold_jid_list != None:           # hold for specific jobs
			holdfor.append(args.hold_jid_list)
		if args.hold:                            # hold for all previous jobs
			prev_jobs = get_hold_jobs()
			holdfor.extend(prev_jobs)
		if len(holdfor) > 0:                    # if there's anything to hold for, actually do a hold ;)
			scripth.write("# Hold for these job numbers, from .slurm_array_jobnums and --hold_jid \n")
			scripth.write("#SBATCH --dependency=afterany:" + ":".join(holdfor) + "\n")
			scripth.write("# \n")
	if args.HOLD:
		scripth.write("# Hold this job until released by scontrol release <JOBID> \n")
		scripth.write("#SBATCH -H \n")
		scripth.write("# \n")

	scripth.write("# Set memory requested and max memory \n")
	if not too_many_commands(args):
		scripth.write("#SBATCH --mem=" + str(args.memory) + "\n")
	else:
		scripth.write("#SBATCH --mem=4gb\n")

	scripth.write("# \n")
	
	scripth.write("# Request some processors \n")
	if not too_many_commands(args):
		scripth.write("#SBATCH --cpus-per-task=" + str(args.processors) + "\n")
	else:
		scripth.write("#SBATCH --ntasks=1\n")
	scripth.write("# \n")
	
	if args.wd != None:
		scripth.write("# Set working directory \n")
		scripth.write("#SBATCH --workdir=" + args.wd + "\n")
		scripth.write("# \n")

	if args.mail != None:
		scripth.write("# Email \n")
		scripth.write("#SBATCH --mail-user=" + str(args.mail) + "\n")
		scripth.write("# Email Type\n")
		scripth.write("#SBATCH --mail-type=" + str(args.mailtype) + "\n")
	
	scripth.write("# Loading specified modules\n")
	scripth.write("# \n")
	if len(args.module) > 0:
		for i in args.module:
			scripth.write("module load " + i + "\n")
	if not too_many_commands(args):
		jobsuffix  = ".$SLURM_ARRAY_JOB_ID_$SLURM_ARRAY_TASK_ID.txt\n"
		outfile = args.rundir + "/command." + jobname + jobsuffix
		scripth.write("# \n")
		scripth.write("echo \"  Started on:           \" `/bin/hostname -s` \n")
		scripth.write("echo \"  Started at:           \" `/bin/date` \n")

		scripth.write("# Run the command through time with memory and such reporting. \n")
		scripth.write("# warning: there is an old bug in GNU time that overreports memory usage \n")
		scripth.write("# by 4x; this is compensated for in the SGE_Plotdir script. \n")
		scripth.write("cmdcmd=`sed \"$((1 + SLURM_ARRAY_TASK_ID)) q;d\" " + args.rundir + "/commands.txt`\n")
		scripth.write("echo \#!/usr/bin/env bash > " + outfile)
		scripth.write("echo $cmdcmd >> " + outfile)
		scripth.write("chmod u+x " + outfile)
		scripth.write("/usr/bin/env time -f \" \\\\tFull Command:                      %C \\\\n\\\\tMemory (kb):                       %M \\\\n\\\\t# SWAP  (freq):                    %W \\\\n\\\\t# Waits (freq):                    %w \\\\n\\\\tCPU (percent):                     %P \\\\n\\\\tTime (seconds):                    %e \\\\n\\\\tTime (hh:mm:ss.ms):                %E \\\\n\\\\tSystem CPU Time (seconds):         %S \\\\n\\\\tUser   CPU Time (seconds):         %U \" \\\n")
		scripth.write(outfile)
		scripth.write("echo \"  Finished at:           \" `date` \n")
	else:
		jobsuffix  = ".%A_%a_%s"
		outfile = args.rundir + "/command." + jobname + ".${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${c}.txt\n"
		scripth.write("# \n")
		scripth.write("echo \"  Started on:           \" `/bin/hostname -s` \n")
		scripth.write("echo \"  Started at:           \" `/bin/date` \n")
		scripth.write("# Run the command through time with memory and such reporting. \n")
		scripth.write("# warning: there is an old bug in GNU time that overreports memory usage \n")
		scripth.write("# by 4x; this is compensated for in the SGE_Plotdir script. \n")
		scripth.write("# \n")
		scripth.write("# This script is running an array that will submit scripts serially. \n")
		scripth.write("# The number of steps is defined by the nsteps variable \n")
		scripth.write("nsteps=" + str(NRUNS) + "\n")
		scripth.write("for (( c = 0; c < nsteps; c++ )) ; do\n")
		scripth.write("	# Use sed to grab a line from commands.txt\n")
		scripth.write("	cmdcmd=`sed -n \"$((1 + SLURM_ARRAY_TASK_ID * nsteps + c)) p\" " + args.rundir + "/commands.txt`\n")
		scripth.write("	if [ -n \"${cmdcmd}\" ] ; then\n")
		# Writing to outfile
		scripth.write("		# Write script to text file, recording the host, and start and end time. \n")
		scripth.write("		printf '#!/usr/bin/env bash\\n' > " + outfile)
		scripth.write("		printf 'echo \"  Started on:           \" `/bin/hostname -s` \\n' >> " + outfile)
		scripth.write("		printf 'echo \"  Started at:           \" `/bin/date` \\n' >> " + outfile)
		scripth.write("		echo $cmdcmd >> " + outfile)
		scripth.write("		printf 'echo \"  Finished at:           \" `date` \\n' >> " + outfile)
		scripth.write("		# Make the file executable\n")
		scripth.write("		chmod u+x " + outfile + "\n")
		scripth.write("		# Run the command in this Slurm array job allocation in a separate\n")
		scripth.write("		# Slurm job step\n")
		scripth.write("		# --------------------------------------------------------------\n")
		# Running the command
		scripth.write("		srun \\\n")
		scripth.write("		--mem=" + args.memory + " \\\n")
		scripth.write("		--time=" + args.time + " \\\n")
		scripth.write("		--cpus-per-task=" + args.processors + " \\\n")
		scripth.write("		--ntasks=1 \\\n")
		scripth.write("		--output=" + args.rundir + "/" + jobname + jobsuffix + ".out \\\n")
		scripth.write("		--error="  + args.rundir + "/" + jobname + jobsuffix + ".err \\\n")
		if args.wd is not None:
			scripth.write("		--workdir=" + args.wd + " \\\n")
		scripth.write("		/usr/bin/env time -f \" \\\\tFull Command:                      %C \\\\n\\\\tMemory (kb):                       %M \\\\n\\\\t# SWAP  (freq):                    %W \\\\n\\\\t# Waits (freq):                    %w \\\\n\\\\tCPU (percent):                     %P \\\\n\\\\tTime (seconds):                    %e \\\\n\\\\tTime (hh:mm:ss.ms):                %E \\\\n\\\\tSystem CPU Time (seconds):         %S \\\\n\\\\tUser   CPU Time (seconds):         %U \" \\\n")
		scripth.write("		" + outfile)
		scripth.write("	else\n\t\techo \"Line $((1 + SLURM_ARRAY_TASK_ID * nsteps + c)) missing from commands.txt, skipping\"\n")
		scripth.write("	fi\n")
		scripth.write("done\n")
		scripth.write("echo \"  Finished at:           \" `date` \n")
			
	scripth.close()



## executes qsub args.rundir/args.rundir.sh
def exec_qsub(args):
	res = ""
	try:
		res = subprocess.check_output("sbatch -Q '" + args.rundir + "/" + os.path.basename(args.rundir) + ".sh'", shell = True)
	except subprocess.CalledProcessError as exc:
		print("Problem submmitting. Are you sure you're on a machine from which SLURM jobs can be submitted? qsub returncode: " + str(exc.returncode))
		shutil.rmtree(args.rundir)
		quit()
	
	jobnum = re.subn("[A-Za-z ]", "", res.strip())[0]
	if args.HOLD:
		print("Successfully submitted job " + jobnum + ", logging job number, timestamp, and rundir to " + SAJ + "\n THIS JOB IS ON HOLD. TO RELEASE, ENTER scontrol release " + jobnum + " IN YOUR TERMINAL")
		jobfile = io.open(args.rundir + "/jobnum.txt", "wb")
		jobfile.write("scontrol release " + jobnum + "_\n")
		jobfile.close()
	else:
		print("Successfully submitted job " + jobnum + ", logging job number, timestamp, and rundir to " + SAJ)
	subprocess.check_output("echo '" + jobnum + "\t" + args.timestamp + "\t" + args.rundir + "' >> " + SAJ, shell = True)

args = parse_input()
SAJ = ".slurm_array_jobnums" 
make_rundir(args.rundir)
write_commands(args.commands, args.rundir)
write_qsub(args)

if not args.debug:
	exec_qsub(args)


