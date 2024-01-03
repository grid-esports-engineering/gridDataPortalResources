""" This script will download one or more games from the LDP,
    clean them, and export a CSV file with the results.

    INSTRUCTIONS FOR USE
    --------------------
    1. Ensure that Python is installed on your computer. You can install
       Python from https://www.python.org/downloads/
    2. Edit the CONFIG object below, adding your API key and modifying
       the settings as desired.
    3. Open a terminal, such as a command prompt or PowerShell on Windows
       or a Bash terminal on a Unix system.
    4. Navigate to the folder that contains this script.
    5. Run the script by entering "python3 lol_basic_parser.py"
    6. Look in the folder for the output file.
"""
import json
import requests
import time
import csv
from datetime import datetime
import sys

CONFIG = {
    "api_key": "",  # You can find your API key in the GRID dashboard
    "filename": "lol_data",  # Enter the name you want to use for the output file
    "include_date_in_file_name": True,  # True or False
    "logging": "on"  # Options: "off", "on"
}
SERIES_IDS_TO_PULL = [
    # ADD SERIES IDS HERE, SEPARATED BY A COMMA.
    # EACH SERIES ID SHOULD BE ENCLOSED IN QUOTES.
    # Example: "12345678", "23456789"
]

SERIES_INFO_QUERY = """
    {
        series (
            id: %s
        ) {
            id
            type
            tournament {
                id
                name
                nameShortened
            }
        }
    }
"""

SERIES_STATE_QUERY = """
    {
        seriesState (
            id: %s
        ) {
            id
            games {
                id
                sequenceNumber
                started
                finished
            }
        }
    }
"""


FIELD_LIST = [
    "platform_game_id",
    "tournament_id",
    "tournament_name",
    "summoner_name",
    "team_tag",
    "side",
    "auto_detect_role",
    "champion",
    "win",
    "game_duration",
    "kills",
    "deaths",
    "assists",
    "kda",
    "kill_participation",
    "team_kills",
    "team_deaths",
    "firstBloodKill",
    "firstBloodAssist",
    "firstBloodVictim",
    "damagePerMinute",
    "damageShare",
    "wardsPlacedPerMinute",
    "wardsClearedPerMinute",
    "controlWardsPurchased",
    "creepScore",
    "creepScorePerMinute",
    "goldEarned",
    "goldEarnedPerMinute",
    "firstTurret",
    "turretKills",
    "turretPlates",
    "firstDragon",
    "dragonKills",
    "firstHerald",
    "riftHeraldKills",
    "baronKills",
    "inhibitorKills",
    "bans"
]


# EVERYTHING BELOW NEEDS TO BE MADE LOL-ORIENTED
class API_Messenger():
    def __init__(self, api_key=None, log_to_terminal=None):
        if not api_key:
            raise RuntimeError("No API key was provided")
        self.headers = {
            "x-api-key": api_key
        }
        self.base_url = "https://api.grid.gg/"
        self.log_to_terminal = log_to_terminal
    
    def get(self, endpoint):
        """ Send get request to Riot API.
        """
        if self.log_to_terminal: print_log_to_terminal("Making REST API call")
        request_url = f"{self.base_url}/{endpoint}"

        try_count = 0
        while True:
            if try_count >= 5:
                raise Exception("API request failed too many times")

            try:
                response = requests.get(request_url, headers=self.headers, timeout=3)
            except requests.exceptions.Timeout:
                if self.log_to_terminal: print_log_to_terminal("API request timed out; retrying")
                try_count += 1
                continue

            if response.status_code == 200:
                if self.log_to_terminal: print_log_to_terminal("API call was successful")
                return response.content
            elif response.status_code == 429:
                if self.log_to_terminal: print_log_to_terminal(f"API rate-limited; sleeping {response.headers.get('Retry-After')}s")
                time.sleep(int(response.headers.get("Retry-After")))
                try_count += 1
                continue
            elif response.status_code == 401:
                if self.log_to_terminal: print_log_to_terminal("API request failed: request was not authorized (401 error)")
                return response.status_code
            elif response.status_code == 403:
                if self.log_to_terminal: print_log_to_terminal("API request failed: access forbidden (403 error)")
                return response.status_code
            elif response.status_code == 404:
                if self.log_to_terminal: print_log_to_terminal(f"Series not found (404 error)")
                return response.status_code
            else:
                if self.log_to_terminal: print_log_to_terminal(f"API request failed: error code {response.status_code}; sleeping and retrying")
                time.sleep(1)
                try_count += 1
                continue
    
    def post(self, query, endpoint="central-data/graphql"):
        """ Method to post a GraphQL request to GRID Central Data.
        """
        if self.log_to_terminal: print_log_to_terminal("Making GraphQL API call")
        request_headers = self.headers
        request_headers["Content-Type"] = "application/json"

        payload = json.dumps({
            "query": query
        })

        response = requests.request(
            "POST",
            f"{self.base_url}/{endpoint}",
            headers=request_headers,
            data=payload
        )

        if response.json().get("errors"):
            raise Exception(f"Query failed: {response.json()['errors'][0]['message']}")

        response_body = response.json()
        return response_body


def print_log_to_terminal(message):
    """ Simple command-line logger.
    """
    timestamp = datetime.now()
    print(f"{timestamp} :: {message}")


def split_team_tag_and_player_nickname(summoner_name, log_to_terminal=False):
    """ Attempts to split the team tag off of the summoner name.
    """
    team_tag_found = False
    team_tag = None
    player_name = summoner_name

    if summoner_name.find(" ") != -1 and summoner_name.find(" ") < 5:
        start_of_summoner_name = summoner_name.split(" ")[0]
        if start_of_summoner_name.isupper():
            team_tag_found = True
            team_tag = start_of_summoner_name
            player_name = summoner_name[summoner_name.find(" ")+1:]
            if log_to_terminal: print(f"Split {summoner_name} into team tag {team_tag} and player name {player_name}")

    if not team_tag_found:
        if log_to_terminal: print(f"Could not detect team tag in {summoner_name}")

    return team_tag, player_name


def game_factory(game_id, series_info, stats_file, timeline_file, live_data, log_to_terminal=False):
    """ Receive full data from the API call and prepare a cleaned array.

        :param: game_id, the game's platform_game_id
        :param: series_info, the basic metadata for the series
        :param: stats_file, the raw Riot postgame stats file
        :param: timeline_file, the raw Riot postgame details file
        :param: live_data, the raw Riot live data file (.jsonl)
        :param: log_to_terminal, optional, whether or not to print detailed logs
        :returns: cleaned_game_data, an array of 12 dicts containing calculated
                  stats from the game for the 10 participating players and the
                  2 participating teams
    """
    cleaned_game_data = []

    team_totals = {
        100: {
            "kills": 0,
            "deaths": 0,
            "damage_to_champions": 0,
            "gold_earned": 0,
            "creep_score": 0,
            "wards_placed": 0,
            "wards_killed": 0,
            "control_wards_purchased": 0,
            "turret_plates": 0
        },
        200: {
            "kills": 0,
            "deaths": 0,
            "damage_to_champions": 0,
            "gold_earned": 0,
            "creep_score": 0,
            "wards_placed": 0,
            "wards_killed": 0,
            "control_wards_purchased": 0,
            "turret_plates": 0
        }
    }

    first_blood_found = False
    first_blood_victim = None
    for frame in timeline_file["frames"]:
        if frame["timestamp"] > 850000:
            break
        for event in frame["events"]:
            if event["type"] == "TURRET_PLATE_DESTROYED":
                if event["teamId"] == 200:
                    team_id = 100
                elif event["teamId"] == 100:
                    team_id = 200
                else:
                    # Shouldn't be possible
                    continue
                team_totals[team_id]["turret_plates"] += 1
            
            if event["type"] == "CHAMPION_KILL":
                if first_blood_found:
                    continue
                if event["killerId"] != 0:
                    first_blood_found = True
                    first_blood_victim = event["victimId"]

    for player in stats_file["participants"]:
        team_totals[player["teamId"]]["kills"] += player["kills"]
        team_totals[player["teamId"]]["deaths"] += player["deaths"]
        team_totals[player["teamId"]]["gold_earned"] += player["goldEarned"]
        team_totals[player["teamId"]]["creep_score"] += player["totalMinionsKilled"] + player["neutralMinionsKilled"]
        team_totals[player["teamId"]]["damage_to_champions"] += player["totalDamageDealtToChampions"]
        team_totals[player["teamId"]]["wards_placed"] += player["wardsPlaced"]
        team_totals[player["teamId"]]["wards_killed"] += player["wardsKilled"]
        team_totals[player["teamId"]]["control_wards_purchased"] += player["visionWardsBoughtInGame"]
    
    for player in stats_file["participants"]:
        team_tag, player_name = split_team_tag_and_player_nickname(player["riotIdGameName"], log_to_terminal)

        player_dto = {
            "platform_game_id": game_id,
            "tournament_id": series_info["tournament_id"],
            "tournament_name": series_info["tournament_name"],
            "summoner_name": player_name,
            "team_tag": team_tag,
            "side": player["teamId"],
            "auto_detect_role": player["teamPosition"],  # May not be 100% reliable
            "champion": player["championName"],
            "win": int(player["win"]),
            "game_duration": stats_file["gameDuration"],
            "kills": player["kills"],
            "deaths": player["deaths"],
            "assists": player["assists"],
            "kda": (player["kills"] + player["assists"]) / (player["deaths"] if player["deaths"] > 0 else 1),
            "kill_participation": (player["kills"] + player["assists"]) / team_totals[player["teamId"]]["kills"],
            "team_kills": team_totals[player["teamId"]]["kills"],
            "team_deaths": team_totals[player["teamId"]]["deaths"],
            "firstBloodKill": int(player["firstBloodKill"]),
            "firstBloodAssist": int(player["firstBloodAssist"]),
            "firstBloodVictim": 1 if player["participantId"] == first_blood_victim else 0,
            "damagePerMinute": player["totalDamageDealtToChampions"] / (stats_file["gameDuration"]/60),
            "damageShare": player["totalDamageDealtToChampions"] / team_totals[player["teamId"]]["damage_to_champions"],
            "wardsPlacedPerMinute": player["wardsPlaced"] / (stats_file["gameDuration"]/60),
            "wardsClearedPerMinute": player["wardsKilled"] / (stats_file["gameDuration"]/60),
            "controlWardsPurchased": player["visionWardsBoughtInGame"],
            "creepScore": player["totalMinionsKilled"] + player["neutralMinionsKilled"],
            "creepScorePerMinute": (player["totalMinionsKilled"] + player["neutralMinionsKilled"]) / (stats_file["gameDuration"]/60),
            "goldEarned": player["goldEarned"],
            "goldEarnedPerMinute": player["goldEarned"] / (stats_file["gameDuration"]/60)
        }

        cleaned_game_data.append(player_dto)

    for team in stats_file["teams"]:
        team_tag = None
        team_id = team["teamId"]
        for player in cleaned_game_data:
            if player["side"] == team_id:
                if player["team_tag"]:
                    team_tag = player["team_tag"]
                    if log_to_terminal: print(f"Found team tag {team_tag} for team {team_id}")
                    break
        if not team_tag:
            if log_to_terminal: print(f"Could not find a team tag for team {team_id}")

        team_dto = {
            "platform_game_id": game_id,
            "tournament_id": series_info["tournament_id"],
            "tournament_name": series_info["tournament_name"],
            "team_tag": team_tag,
            "side": team_id,
            "win": int(team["win"]),
            "gameDuration": stats_file["gameDuration"],
            "teamKills": team["objectives"]["champion"]["kills"],
            "teamDeaths": team_totals[team_id]["deaths"],
            "firstBloodKill": int(team["objectives"]["champion"]["first"]),
            "wardsPlacedPerMinute": team_totals[team_id]["wards_placed"] / (stats_file["gameDuration"]/60),
            "wardsClearedPerMinute": team_totals[team_id]["wards_killed"] / (stats_file["gameDuration"]/60),
            "controlWardsPurchased": team_totals[team_id]["control_wards_purchased"],
            "creepScorePerMinute": team_totals[team_id]["creep_score"] / (stats_file["gameDuration"]/60),
            "goldEarnedPerMinute": team_totals[team_id]["gold_earned"] / (stats_file["gameDuration"]/60),
            "firstTurret": int(team["objectives"]["tower"]["first"]),
            "turretKills": team["objectives"]["tower"]["kills"],
            "turretPlates": team_totals[team_id]["turret_plates"],
            "firstDragon": int(team["objectives"]["dragon"]["first"]),
            "dragonKills": team["objectives"]["dragon"]["kills"],
            "firstHerald": int(team["objectives"]["riftHerald"]["first"]),
            "riftHeraldKills": team["objectives"]["riftHerald"]["kills"],
            "baronKills": team["objectives"]["baron"]["kills"],
            "inhibitorKills": team["objectives"]["inhibitor"]["kills"],
            "bans": team["bans"]  # Examples from recent scrim data are all empty arrays...
        }
        """
        "ban1": None,  # TODO: Use live data file?
        "ban2": None,
        "ban3": None,
        "ban4": None,
        "ban5": None,
        "pick1": None,  # TODO: Use live data file?
        "pick2": None,
        "pick3": None,
        "pick4": None,
        "pick5": None
        """

        cleaned_game_data.append(team_dto)

    return cleaned_game_data


def main(log_to_terminal):
    """ Main function.
    """
    start_time = datetime.now()

    api = API_Messenger(
        api_key=CONFIG["api_key"],
        log_to_terminal=log_to_terminal
    )

    if len(SERIES_IDS_TO_PULL) < 1:
        print("No series IDs were provided. Please edit the Python file and add series IDs where specified.")
        sys.exit()

    # Move forward with processing series
    output_array = []

    for series_id in SERIES_IDS_TO_PULL:
        print_log_to_terminal(f"Starting series {series_id}")
        # Get series info from Central Data
        query = SERIES_INFO_QUERY % series_id
        try:
            response = api.post(query)
            series_from_central_data = response["data"]["series"]
            if series_from_central_data["tournament"]["name"] == "League of Legends Scrims":
                tournament_name = "Scrim"
            else:
                tournament_name = series_from_central_data["tournament"]["name"]

            series_metadata = {
                "series_id": series_id,
                "tournament_id": series_from_central_data["tournament"]["id"],
                "tournament_name": tournament_name,
                "games": []
            }

            # Now go to series state to get the list of individual games in the series
            query = SERIES_STATE_QUERY % series_id
            response = api.post(query, endpoint="live-data-feed/series-state/graphql")
            if log_to_terminal: print_log_to_terminal(f"Found {len(response['data']['seriesState']['games'])} "
                                                      f"games in series {series_id}")
            for game in response["data"]["seriesState"]["games"]:
                series_metadata["games"].append(game)
        except Exception as error:
            print_log_to_terminal(f"Could not fetch metadata for series {series_id}: {str(error)}")
            continue

        # This is where the download and parse for each game happens
        for game in series_metadata["games"]:
            # Fetch data files
            sequence_number = game["sequenceNumber"]
            stats_endpoint = f"file-download/end-state/riot/series/{series_id}/games/{sequence_number}/summary"
            timeline_endpoint = f"file-download/end-state/riot/series/{series_id}/games/{sequence_number}/details"
            live_endpoint = f"file-download/events/riot/series/{series_id}/games/{sequence_number}"

            # Do downloads
            stats_file = api.get(stats_endpoint)
            timeline_file = api.get(timeline_endpoint)
            live_file = api.get(live_endpoint)

            # Convert responses into dict objects
            stats_file = json.loads(stats_file)
            timeline_file = json.loads(timeline_file)
            live_data = live_file.decode(encoding="utf-8")

            # Parse
            game_id = f"{stats_file['platformId']}_{stats_file['gameId']}"
            if log_to_terminal: print_log_to_terminal(f"Sending game {game_id} to parser")
            cleaned_game_data = game_factory(
                game_id,
                series_metadata,
                stats_file,
                timeline_file,
                live_data,
                log_to_terminal=log_to_terminal
            )

            # Append results into output_array
            output_array.extend(cleaned_game_data)
        
        print_log_to_terminal(f"Finished parsing games in series {series_id}")

    # Dump the output_array to a CSV and save to disk
    date_string = f"_{datetime.now().strftime('%Y%m%d_%H%M') if CONFIG['include_date_in_file_name'] else ''}"
    filename_to_use = f"{CONFIG['filename']}{date_string}.csv"
    with open(filename_to_use, "w", newline="") as file:
        csv_writer = csv.writer(file, delimiter=',')
        csv_writer.writerow(FIELD_LIST)
        for row in output_array:
            csv_writer.writerow([
                row.get(field) for field in FIELD_LIST
            ])

    runtime = str(datetime.now() - start_time).split(".")[0]
    print(f"Zug zug; job's done :: Runtime: {runtime}")


if __name__ == "__main__":
    print("Starting the GRID Valorant flat-file generator")
    if bool(CONFIG["logging"] == "on"):
        print("Detailed logging to terminal enabled")
        log_to_terminal = True
    else:
        log_to_terminal = False

    main(log_to_terminal)
