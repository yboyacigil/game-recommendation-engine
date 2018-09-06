"""
Send query
"""

import predictionio
import argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Send query")
  parser.add_argument('--url', default="http://localhost:8000")
  parser.add_argument('--player')
  parser.add_argument('--games')

  args = parser.parse_args()
  if args.player and args.games:
    engine_client = predictionio.EngineClient(args.url)
    print engine_client.send_query({
      "player": args.player,
      "games": args.games.split(",")
      })
  else:
    print 'ERROR: Invalid args!', args
