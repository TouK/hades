#!/bin/bash
#
# Copyright 2011 TouK
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: msk@touk.pl
# Extracts lines with hades logs from the given file. For each line prints following data:
# <number of seconds> <execution time for main db> <main db name> <execution time for failover db> <failover db name> <currently used db name>

function usage {
    echo "Usage: $(basename $0) <log file>" >&2
    exit 1
}

if [ $# -ne 1 ]; then usage; fi

function extract_active_db_from_state {
    echo $1|tr ' ' '\n'|tac|head -1|tr -d ')'
}

function time_to_seconds {
    while read HOUR MINUTE SECOND T1 DB1 T2 DB2 STATE; do
	((SECONDS = 10#$HOUR * 3600 + 10#$MINUTE * 60 + 10#$SECOND))
	echo $SECONDS $T1 $DB1 $T2 $DB2 using $(extract_active_db_from_state "$STATE")
    done
}

FIELD_TIME=2
FIELD_EXEC_TIME_MAIN_DB=27
FIELD_MAIN_DB_NAME=30
FIELD_EXEC_TIME_FAILOVER_DB=34
FIELD_FAILOVER_DB_NAME=37
FIELDS_STATE=51-

cat $1 | grep -E "average execution time for" | tr -s ' '|cut -d' ' -f$FIELD_TIME,$FIELD_EXEC_TIME_MAIN_DB,$FIELD_MAIN_DB_NAME,$FIELD_EXEC_TIME_FAILOVER_DB,$FIELD_FAILOVER_DB_NAME,$FIELDS_STATE|tr -d ',;' | tr ':' ' ' |time_to_seconds
