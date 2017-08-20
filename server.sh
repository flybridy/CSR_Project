#! /bin/sh
source /home/flybridy/.bash_profile
pid_file='server.pid'
start()
{
        echo $"Starting main server ......"
        java  -Xms50m -Xmx1024m -XX:-UseGCOverheadLimit -jar ./LabServer.jar labserver > /dev/null &
        echo $! > $pid_file
        echo $"main server started!"
}

stop()
{
        echo $"Stopping main server ......"
        pid=`cat $pid_file`
        kill -9 $pid
        echo "stop "$pid
        mv log/day.log log/day.log.bak_`date +%m%d%H%M`
        sleep 1
}

restart()
{
        stop
        sleep 5
        start
}

case "$1" in
start)
        start
        ;;
stop)
        stop
        ;;
restart)
        restart
        ;;
*)
        echo $"Usage: $0 {start|stop|restart}"
        exit 1
esac
