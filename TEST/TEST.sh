#!/usr/bin/env bash
#
# This file created by SLURM_Array
#
# 
# Set job name 
#SBATCH --job-name=TEST
# 
# Set job time 
#SBATCH --time=4-0:00:00
# 
# Set array job range (1 to number of commands in cmd file) and concurrency (%N) 
#SBATCH --array=1-4
# 
# Output files for stdout and stderr 
#SBATCH --output=TEST/TEST.%A_%a.out
#SBATCH --error=TEST/TEST.%A_%a.err
# 
# Set memory requested and max memory 
#SBATCH --mem=4gb
# 
# Request some processors 
#SBATCH --cpus-per-task=1
# 
# Loading specified modules
# 
# 
echo "  Started on:           " `/bin/hostname -s` 
echo "  Started at:           " `/bin/date` 
# Run the command through time with memory and such reporting. 
# warning: there is an old bug in GNU time that overreports memory usage 
# by 4x; this is compensated for in the SGE_Plotdir script. 
nruns=4
for (( c = 0; c < nruns+1; c++ )) ; do
	cmdcmd=`sed -n "$((SLURM_ARRAY_TASK_ID * nruns + c)) p" TEST/commands.txt`
	if [ -n "${cmdcmd}" ] ; then
		printf '#!/usr/bin/env bash\n' > TEST/command.TEST.%A_%a_$c.txt
		printf 'echo "  Started on:           " `/bin/hostname -s` \n' >> TEST/command.TEST.%A_%a_$c.txt
		printf 'echo "  Started at:           " `/bin/date` \n' >> TEST/command.TEST.%A_%a_$c.txt
		echo $cmdcmd >> TEST/command.TEST.%A_%a_$c.txt
		printf 'echo "  Finished at:           " `date` \n' >> TEST/command.TEST.%A_%a_$c.txt
		# run the command in this Slurm array job allocation in a separate
		# Slurm job step
		# --------------------------------------------------------------
		chmod u+x TEST/command.TEST.%A_%a_$c.txt

		srun \
			--mem=4gb \
			--time=04:00:00 \
			--cpus-per-task=1 \
			--output=TEST/TEST.%A_%a_%s.out \
			--error=TEST/TEST.%A_%a_%s.err \
			/usr/bin/env time -f " \\tFull Command:                      %C \\n\\tMemory (kb):                       %M \\n\\t# SWAP  (freq):                    %W \\n\\t# Waits (freq):                    %w \\n\\tCPU (percent):                     %P \\n\\tTime (seconds):                    %e \\n\\tTime (hh:mm:ss.ms):                %E \\n\\tSystem CPU Time (seconds):         %S \\n\\tUser   CPU Time (seconds):         %U " \
			TEST/command.TEST.%A_%a_$c.txt
	else
		echo "Line $((SLURM_ARRAY_TASK_ID * nruns + c)) missing from commands.txt, skipping"
	fi
done
echo "  Finished at:           " `date` 
