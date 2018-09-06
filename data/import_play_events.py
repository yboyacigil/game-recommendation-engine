"""
Import play events
"""

import predictionio
import argparse
import random
import csv
from datetime import datetime
import pytz

def import_play_events(client):
  print "Importing play events..."

  count = 0
  with open("play_event_list.csv") as csv_file:
    reader = csv.DictReader(csv_file)
    for row in reader:
      player_id = row["playerId"]
      game_id = row["gameId"]
      time = row["time"]

      print "Player", player_id ,"plays game", game_id
      client.create_event(
        event="play",
        entity_type="player",
        entity_id=player_id,
        target_entity_type="game",
        target_entity_id=game_id,
        event_time=datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.timezone('Europe/Malta'))
      )
      count += 1

  print "%s play events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Import play events")
  parser.add_argument('--access_key', default='invalid access key')
  parser.add_argument('--url', default="http://localhost:7070")

  args = parser.parse_args()

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)

  import_play_events(client)
