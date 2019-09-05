#! /bin/sh
cd /root/workspace/screen/db

nohup /root/anaconda2/bin/python -u send_mail.py > log/send_data.log 2>&1 &
