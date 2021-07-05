#!/bin/bash
cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
CURRENT_DIR=$(cd $(dirname $0); pwd)
python3 $CURRENT_DIR/projects/manage.py --project_env=$PROFILE