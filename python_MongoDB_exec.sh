[ $(date +\%d) -le 07 ] && python /path_location/python_MongoDB_Dump.py -d new_feed -o /destination_path
[ $(date +\%d) -ge 07 ] && echo -ne "\n" && echo "nao e o primeiro sabado do mes, encerrando o script!" && echo -ne "\n"
#echo "it's not the first saturday of the month, ending the script!"
