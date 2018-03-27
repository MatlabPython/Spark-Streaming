appid=$(yarn application --list | grep WaterFlowStreaming | awk '{print $1}')
echo "kill yarn application $appid"
yarn application --kill $appid

