cat ./conf/master | while read line
do
  array=($line)
  ipaddr=${array[0]}
  port=${array[1]}
  user=${array[2]}
  addr=${array[3]}
  ssh -Nf -L $port:$ipaddr:8080 $user@$addr
  port=$((port+1))
  ssh -Nf -L $port:$ipaddr:4040 $user@$addr
done

port=10001

cat ./conf/slaves | while read line
do
  array=($line)
  ipaddr=${array[0]}
  port=$((port+1))
#  port=${array[1]}
  user=${array[2]}
  addr=${array[3]}
  ssh -Nf -L $port:$ipaddr:8081 $user@$addr
  port=$((port+1))
  ssh -Nf -L $port:$ipaddr:4040 $user@$addr
done


#cat ./conf/slaves | while read line
#do
#  array=($line)
#  ipaddr=${array[0]}
#  port=${array[1]}
#  user=${array[2]}
#  addr=${array[3]}
#  port=$((port+3))
#  ssh -Nf -L $port:$ipaddr:4040 $user@$addr
#done
#
#cat ./conf/slaves | while read line
#do
#  array=($line)
#  ipaddr=${array[0]}
#  port=${array[1]}
#  user=${array[2]}
#  addr=${array[3]}
#  port=$((port+6))
#  ssh -Nf -L $port:$ipaddr:9999 $user@$addr
#done
