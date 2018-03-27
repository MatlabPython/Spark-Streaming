appid=$(yarn application --list | grep BridgeStreaming | awk '{print $1}')
echo "kill yarn application $appid"
yarn application --kill $appid

