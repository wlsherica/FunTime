#!/bin/sh

#############################################################################
#    Objectives: A sciprt to execute StarterDIY Athena Interval Luigi Class -> NES testing for incremental algorithms
#  CreationDate: 2015/03/10
#         Usage: ./startdiyflow_interval.sh -d <date> -m
#############################################################################

date=
while getopts "d:m" opt; do
  case $opt in
    d)
      date=$OPTARG
      ;;
    m)
      month=1
      ;;
  esac
done

if [ -z ${date} ]; then
    date=$(date +%Y%m%d)
fi

username=$(whoami)

# Program Lib Path
prog_path=/data/migo/athena/lib/util
prog_data_prepare=${prog_path}/data_prepare.py
prog_interval=${prog_path}/interval_testing.py
prog_nlp=${prog_path}/nlp_testing.py

# HDFS Data Path
data_prepare_data_path=/user/athena/data_prepare_member

data_path=/user/${username}
adjusted_data_path=${data_path}/adjusted_interval
incremental_data_path=${data_path}/incremental_interval
mu_data_path=${data_path}/mu_interval
mu2_data_path=${data_path}/mu2_interval
lrfm_data_path=${data_path}/lrfm
nes_data_path=${data_path}/nes_tag_week
nlp_data_path=${data_path}/nlp_week

# Execute the Data Prepare Luigi Class
data_prepare_src="/user/migo/starterDIY/*/transaction/done/${date}/*/*"
for type in $(echo Member Item);
do
    dest=${data_path}/$(echo ${type} | awk '{print tolower($0)}')
    python ${prog_data_prepare} DataPrepare${type} --use-hadoop --keep-temp --cal-date ${date} --src ${data_prepare_src} --dest ${dest} &
done
wait

# Execute the AdjustedInterval Luigi Class
python ${prog_interval} AdjustedInterval --use-hadoop --keep-temp --cal-date ${date} --src-incremental-interval "${data_prepare_data_path}/${date}" --src-base-incremental-interval ${incremental_data_path}/${yesterday} --dest-incremental-interval ${incremental_data_path}/${date} --dest-mu2 ${mu2_data_path}/${date} --dest-mu ${mu_data_path}/${date} --dest ${adjusted_data_path}/${date} --worker 2

# added by Erica Li

python ${prog_nlp} CalNLP --use-hadoop --keep-temp --cal-date ${date} --period ${period} --cal-date-lrfm ${yesterday} --src-adjusted ${adjusted_data_path}/${date} --src-nes "${adjusted_data_path}/${date}~${lrfm_data_path}/${date}" --src-lrfm "${lrfm_data_path}/${yesterday};${data_prepare_data_path}/${date}" --dest-nes ${nes_data_path}/${date} --dest-lrfm ${lrfm_data_path}/${date} --dest ${nlp_data_path}/${date} --worker 2



# Execute the CalNLP Luigi Class
#if [ -z ${month} ]; then
#    python ${prog_nlp} CalNLP --use-hadoop --keep-temp --cal-date ${date} --period ${period} --cal-date-lrfm ${yesterday} --src-adjusted ${adjusted_data_path}/${date} --src-nes "${adjusted_data_path}/${date}~${lrfm_data_path}/${date}" --src-lrfm "${lrfm_data_path}/${yesterday};${data_prepare_data_path}/${date}" --dest-nes ${nes_data_path}/${date} --dest-lrfm ${lrfm_data_path}/${date} --dest ${nlp_data_path}/${date} --worker 2
#else
#    period=$(echo ${date} | cut -c7-8)
#    shift=$(expr ${period} \* 86400)
#    month_shift=$(expr ${timestamp} - ${shift})
#    yesterday=$(date -d @${month_shift} +'%Y%m%d')

#    nes_data_path=${data_path}/nes_tag_month
#    nlp_data_path=${data_path}/nlp_month

#    python ${prog_nlp} CalNLP --use-hadoop --keep-temp --cal-date ${date} --period ${period} --cal-date-lrfm ${yesterday} --src-adjusted ${adjusted_data_path}/${date} --src-nes "${adjusted_data_path}/${date}~${lrfm_data_path}/${date}" --src-lrfm "${lrfm_data_path}/${yesterday};${data_prepare_data_path}/${date}" --dest-nes ${nes_data_path}/${date} --dest-lrfm ${lrfm_data_path}/${date} --dest ${nlp_data_path}/${date} --worker 2
#fi

