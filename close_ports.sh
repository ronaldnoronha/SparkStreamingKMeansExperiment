
ps aux | grep ssh | while read line
do
  array=($line)
  if [ ${array[10]} == "ssh" -a ${array[11]} == "-Nf" ] ; then
    kill -9 ${array[1]}
  fi
done
