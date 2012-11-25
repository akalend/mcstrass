gcc mcstress.c -march=`uname -m|tr '_' '-'` -O3 -g -pthread -o mcstress

