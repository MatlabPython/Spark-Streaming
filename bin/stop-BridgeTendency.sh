appid=$(yarn application --list | grep BridgeTendency | awk '{print $1}')
echo "kill yarn application $appid"
yarn application --kill $appid

