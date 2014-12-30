#!/bin/bash

#0 3,21 * * * echo "'date': tmuxEnvSaver is running" >> /home/erica_li/mylog/cron-tmux.log 2>&1
#0 3,21 * * * /home/shuai/tmuxEnvSaver.sh >> /home/erica_li/mylog/cron-tmux.log 2>&1
#@reboot /home/erica_li/tmuxEnvSaver.sh >> /home/erica_li/mylog/cron-tmux.log 2>&1
#note: reboot means run this job after reboot
# Reference: http://segmentfault.com/blog/galoisplusplus/1190000000457385

tmuxSnapshot=/.tmux_snapshot
tmuxEXE=/usr/local/bin/tmux
save_snap()
{
        ${tmuxEXE} list-windows -a -F"#{session_name} #{window_name} #{pane_current_command} #{pane_current_path}" > ${tmuxSnapshot}
}
# 列出現有tmux windows, 例如 : python2.7  (1 panes) [145x38] [layout cbfd,145x38,0,0,0] @0

restore_snap()
{
        ${tmuxEXE} start-server
        while IFS=' ' read -r session_name window_name pane_current_command pane_current_path
        do
                ${tmuxEXE} has-session -t "${session_name}" 2>/dev/null
                if [ $? != 0 ]
                then
                        ${tmuxEXE} new-session -d -s "${session_name}" -n ${window_name}
                else
                        ${tmuxEXE} new-window -d -t ${session_name} -n "${window_name}"
                fi
                ${tmuxEXE} send-keys -t "${session_name}:${window_name}" "cd ${pane_current_path}; echo \"Hint: last time you are executing '${pane_current_command}'.\"" ENTER
        done < ${tmuxSnapshot}
}

#自動判斷要做snapshot或是restore 
ps aux|grep -w tmux|grep -v grep
if [ $? != 0 ]
then
        restore_snap
else
        save_snap
fi
