let services_up = ["astarte-data-updater-plant", "astarte-appengine-api", "vernemq", "traefik", "scylla"]
let services_down = ["astarte-data-updater-plant", "astarte-appengine-api", "vernemq"]

docker compose down
docker compose up -d ...$services_up

exit 0


loop {
  try {
    retry -d 5 -t 10 curl -s -X GET http://api.astarte.localhost/appengine/health

    # docker came up, retry
    docker compose down ...$services_down
    docker compose up -d ...$services_up
  } catch {
    print "found it!"
    break 
  }
}
