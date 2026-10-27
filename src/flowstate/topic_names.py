"""Kafka topic names. One definition. Importers do not repeat the strings."""

TOPIC_BY_MODE = {
    "ride_hail": "flowstate.ridehail.activity.v1",
    "metro": "flowstate.metro.station_hour.v1",
    "bus": "flowstate.bus.scheduled_stop.v1",
    "aviation": None,  # arrival and departure are separate topics
}

AVIATION_TOPICS = {
    "arrival": "flowstate.aviation.arrival.v1",
    "departure": "flowstate.aviation.departure.v1",
}
