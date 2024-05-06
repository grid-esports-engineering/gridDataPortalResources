""" This script will download one or more games from the VDP,
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
    5. Run the script by entering "python3 val_basic_parser.py"
    6. Look in the folder for the output file.
"""
import json
import requests
import time
import csv
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile
import sys

CONFIG = {
    "api_key": "",  # You can find your API key in the GRID dashboard
    "filename": "valorant_data",  # Enter the name you want to use for the output file
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


class API_Messenger():
    def __init__(self, api_key=None, log_to_terminal=None):
        if not api_key:
            raise RuntimeError("No API key was provided")
        self.headers = {
            "x-api-key": api_key
        }
        self.base_url = "https://api.grid.gg/"
        self.log_to_terminal = log_to_terminal

    def get(self, series_id, endpoint="file-download/end-state/riot/series"):
        """ Send get request to Riot API.
        """
        if self.log_to_terminal:
            print_log_to_terminal("Making REST API call")
        request_url = f"{self.base_url}/{endpoint}/{series_id}"

        try_count = 0
        while True:
            if try_count >= 5:
                raise Exception("API request failed too many times")

            try:
                response = requests.get(request_url, headers=self.headers, timeout=3)
            except requests.exceptions.Timeout:
                if self.log_to_terminal:
                    print_log_to_terminal("API request timed out; retrying")
                try_count += 1
                continue

            if response.status_code == 200:
                if self.log_to_terminal:
                    print_log_to_terminal("API call was successful")
                return response.content
            elif response.status_code == 429:
                if self.log_to_terminal:
                    print_log_to_terminal(f"API rate-limited; sleeping {response.headers.get('Retry-After')}s")
                time.sleep(int(response.headers.get("Retry-After")))
                try_count += 1
                continue
            elif response.status_code == 401:
                if self.log_to_terminal:
                    print_log_to_terminal("API request failed: request was not authorized (401 error)")
                return response.status_code
            elif response.status_code == 403:
                if self.log_to_terminal:
                    print_log_to_terminal("API request failed: access forbidden (403 error)")
                return response.status_code
            elif response.status_code == 404:
                if self.log_to_terminal:
                    print_log_to_terminal(f"Series with ID {series_id} was not found (404 error)")
                return response.status_code
            else:
                if self.log_to_terminal:
                    print_log_to_terminal(f"API request failed: error code {response.status_code}; "
                                          "sleeping and retrying")
                time.sleep(1)
                try_count += 1
                continue

    def post(self, query):
        """ Method to post a GraphQL request to GRID Central Data.
        """
        if self.log_to_terminal:
            print_log_to_terminal("Making GraphQL API call")
        request_headers = self.headers
        request_headers["Content-Type"] = "application/json"

        payload = json.dumps({
            "query": query
        })

        response = requests.request(
            "POST",
            f"{self.base_url}/central-data/graphql",
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


def game_metadata_factory(game_data_from_grid_endstate):
    """ Receives game data from the GRID endstate file
        and prepares a basic overview and a map of the teams
        and players involved.

        :param: game_data_from_grid_endstate, object from GRID end-state
        :returns: cleaned_game_metadata, dict of basic game metadata
    """
    if not game_data_from_grid_endstate["started"]:
        raise ValueError("Game has not yet started; try again later")
    if not game_data_from_grid_endstate["finished"]:
        raise ValueError("Game is not yet complete; try again later")

    metadata = {
        "map_name": game_data_from_grid_endstate["map"]["name"].capitalize(),
        "game_number": game_data_from_grid_endstate["sequenceNumber"],
        "team_one": {},
        "team_two": {}
    }

    for idx, team in enumerate(game_data_from_grid_endstate["teams"]):
        if idx == 0:
            team_ref = "team_one"
        elif idx == 1:
            team_ref = "team_two"
        else:
            raise IndexError("Encountered more than two teams")

        metadata[team_ref]["id"] = team["id"]
        metadata[team_ref]["name"] = team["name"]
        metadata[team_ref]["winner"] = team["won"]
        metadata[team_ref]["rounds_won"] = team["score"]

    return metadata


def game_factory(raw_game_data, series_metadata, val_metadata, log_to_terminal=False):
    """ Receive full data from the API call and prepare a cleaned array.

        :param: raw_game_data, the raw Riot postgame data
        :param: series_metadata, a dict containing precleaned basic metadata
                from the series
        :returns: cleaned_game_data, an array of 12 dicts containing calculated
                  stats from the game for the 10 participating players and the
                  2 participating teams
    """
    game_id = raw_game_data["matchInfo"]["matchId"]
    map_id = raw_game_data["matchInfo"]["mapId"]
    map_name = val_metadata["maps"][map_id]["displayName"]
    game_start = datetime.fromtimestamp(raw_game_data["matchInfo"]["gameStartMillis"] / 1000)

    # Clean up game version (patch) string to make it more readable
    start_simple_gm_ver = raw_game_data["matchInfo"]["gameVersion"].find("-") + 1
    end_simple_gm_vr = raw_game_data["matchInfo"]["gameVersion"].find("-", start_simple_gm_ver + 1)
    game_version_clean = float(raw_game_data["matchInfo"]["gameVersion"][start_simple_gm_ver:end_simple_gm_vr])

    match_found = False
    for game_metadata in series_metadata["games"]:
        if game_metadata["map_name"] == map_name:
            if log_to_terminal:
                print_log_to_terminal(f"Matched game {game_id} on map name ({map_name})")
            match_found = True
            this_game_metadata = game_metadata
            break
    if not match_found:
        raise ValueError("Unable to match game to metadata")

    # Match team info from GRID end-state to teams in Riot game data
    team_side_refs = {
        "Blue": None,
        "Red": None
    }
    for team in raw_game_data["teams"]:
        if team["roundsWon"] == this_game_metadata["team_one"]["rounds_won"]:
            if log_to_terminal:
                print_log_to_terminal(f"Team {team['teamId']} is team_one")
            team_side_refs[team["teamId"]] = "team_one"
        elif team["roundsWon"] == this_game_metadata["team_two"]["rounds_won"]:
            if log_to_terminal:
                print_log_to_terminal(f"Team {team['teamId']} is team_two")
            team_side_refs[team["teamId"]] = "team_two"
        else:
            raise ValueError(f"Failed to map {team['teamId']} onto a team from "
                             "the GRID metadata")

    # Pre-aggregation
    player_preaggregated_stats = {}
    team_preaggregated_stats = {
        "Blue": {
            "attackWins": 0,
            "attackLosses": 0,
            "defenseWins": 0,
            "defenseLosses": 0,
        },
        "Red": {
            "attackWins": 0,
            "attackLosses": 0,
            "defenseWins": 0,
            "defenseLosses": 0,
        }
    }

    for idx, round_data in enumerate(raw_game_data["roundResults"]):
        round_number = idx + 1
        if round_number < 25:
            attackers = "Red" if round_number < 13 else "Blue"
        else:
            attackers = "Red" if round_number % 2 == 1 else "Blue"

        if attackers == "Blue":
            if round_data["winningTeam"] == "Blue":
                team_preaggregated_stats["Blue"]["attackWins"] += 1
                team_preaggregated_stats["Red"]["defenseLosses"] += 1
            else:
                team_preaggregated_stats["Blue"]["attackLosses"] += 1
                team_preaggregated_stats["Red"]["defenseWins"] += 1
        elif attackers == "Red":
            if round_data["winningTeam"] == "Red":
                team_preaggregated_stats["Red"]["attackWins"] += 1
                team_preaggregated_stats["Blue"]["defenseLosses"] += 1
            else:
                team_preaggregated_stats["Red"]["attackLosses"] += 1
                team_preaggregated_stats["Blue"]["defenseWins"] += 1

        round_kill_events = []
        # Upon encountering a player, check if their PUUID is in player_preaggregated_stats
        # and add them if it isn't. Then increment values into their dict.
        for player in round_data["playerStats"]:
            if not player_preaggregated_stats.get(player["puuid"]):
                player_preaggregated_stats[player["puuid"]] = {
                    "total_damage": 0,
                    "headshots": 0,
                    "bodyshots": 0,
                    "legshots": 0,
                    "first_kills": 0,
                    "first_deaths": 0
                }

            for target in player["damage"]:
                player_preaggregated_stats[player["puuid"]]["total_damage"] += target["damage"]
                player_preaggregated_stats[player["puuid"]]["headshots"] += target["headshots"]
                player_preaggregated_stats[player["puuid"]]["bodyshots"] += target["bodyshots"]
                player_preaggregated_stats[player["puuid"]]["legshots"] += target["legshots"]

            for kill in player["kills"]:
                round_kill_events.append(kill)

        # Now sort the array of kills and store the first kill / death PUUIDs
        sorted_kills = sorted(
            round_kill_events,
            key=lambda kill: kill["timeSinceRoundStartMillis"]
        )

        player_preaggregated_stats[sorted_kills[0]["killer"]]["first_kills"] += 1
        player_preaggregated_stats[sorted_kills[0]["victim"]]["first_deaths"] += 1

    # Start generating output data
    cleaned_game_data = []
    team_rows_added_map = {
        "Blue_team_row_added": False,
        "Red_team_row_added": False
    }

    # Create the 5 players and 1 team row for each team
    player_count_found = 0
    for player in raw_game_data["players"]:
        if player["teamId"] == "Neutral":
            continue

        player_count_found += 1
        player_id = player["puuid"]
        rounds_won = this_game_metadata[team_side_refs[player["teamId"]]]["rounds_won"]
        player_row = {
            "game_id": game_id,
            "series_id": series_metadata["series_id"],
            "tournament_id": series_metadata["tournament_id"],
            "tournament_name": series_metadata["tournament_name"],
            "map_id": map_id,
            "map_name": map_name,
            "game_start": game_start,
            "game_version": game_version_clean,
            "game_number": this_game_metadata["game_number"],
            "player_name": player["gameName"],
            "team_id": this_game_metadata[team_side_refs[player["teamId"]]]["id"],
            "team_name": this_game_metadata[team_side_refs[player["teamId"]]]["name"],
            "agent_id": player["characterId"],
            "agent_name": val_metadata["agents"][player["characterId"]]["displayName"],
            "win": 1 if this_game_metadata[team_side_refs[player["teamId"]]]["winner"] else 0,
            "roundsWon": rounds_won,
            "roundsLost": player["stats"]["roundsPlayed"] - rounds_won,
            "attackRoundsWon": team_preaggregated_stats[player["teamId"]]["attackWins"],
            "attackRoundsLost": team_preaggregated_stats[player["teamId"]]["attackLosses"],
            "defenseRoundsWon": team_preaggregated_stats[player["teamId"]]["defenseWins"],
            "defenseRoundsLost": team_preaggregated_stats[player["teamId"]]["defenseLosses"],
            "kills": player["stats"]["kills"],
            "deaths": player["stats"]["deaths"],
            "assists": player["stats"]["assists"],
            "averageCombatScore": round(player["stats"]["score"] / player["stats"]["roundsPlayed"], 1),
            "damagePerRound": round(
                player_preaggregated_stats[player_id]["total_damage"]
                /
                player["stats"]["roundsPlayed"],
                1
            ),
            "first_kills": player_preaggregated_stats[player_id]["first_kills"],
            "first_deaths": player_preaggregated_stats[player_id]["first_deaths"],
            "headshot_rate": round((
                player_preaggregated_stats[player_id]["headshots"]
                /
                (
                    player_preaggregated_stats[player_id]["headshots"]
                    + player_preaggregated_stats[player_id]["bodyshots"]
                    + player_preaggregated_stats[player_id]["legshots"]
                )
            ), 3)
        }
        cleaned_game_data.append(player_row)

        # If there is no Team row for this team yet, construct one
        if not team_rows_added_map[f"{player['teamId']}_team_row_added"]:
            if log_to_terminal:
                print_log_to_terminal(f"Adding row for {player['teamId']} team")
            # Null strings are added in some places to ensure the CSV-write is
            # clean, by forcing the correct number of columns
            team_row = {
                "game_id": game_id,
                "series_id": series_metadata["series_id"],
                "tournament_id": series_metadata["tournament_id"],
                "tournament_name": series_metadata["tournament_name"],
                "map_id": map_id,
                "map_name": map_name,
                "game_start": game_start,
                "game_version": game_version_clean,
                "game_number": this_game_metadata["game_number"],
                "player_name": "",
                "team_id": this_game_metadata[team_side_refs[player["teamId"]]]["id"],
                "team_name": this_game_metadata[team_side_refs[player["teamId"]]]["name"],
                "agent_id": "",
                "agent_name": "",
                "win": 1 if this_game_metadata[team_side_refs[player["teamId"]]]["winner"] else 0,
                "roundsWon": rounds_won,
                "roundsLost": player["stats"]["roundsPlayed"] - rounds_won,
                "attackRoundsWon": team_preaggregated_stats[player["teamId"]]["attackWins"],
                "attackRoundsLost": team_preaggregated_stats[player["teamId"]]["attackLosses"],
                "defenseRoundsWon": team_preaggregated_stats[player["teamId"]]["defenseWins"],
                "defenseRoundsLost": team_preaggregated_stats[player["teamId"]]["defenseLosses"],
                "kills": "",
                "deaths": "",
                "assists": "",
                "averageCombatScore": "",
                "damagePerRound": "",
                "first_kills": "",
                "first_deaths": "",
                "headshot_rate": ""
            }
            cleaned_game_data.append(team_row)
            team_rows_added_map[f"{player['teamId']}_team_row_added"] = True

    if player_count_found < 10:
        raise ValueError("Found fewer than 10 non-Neutral players")
    if not team_rows_added_map["Blue_team_row_added"]:
        raise ValueError("Failed to create a team row for Blue team")
    if not team_rows_added_map["Red_team_row_added"]:
        raise ValueError("Failed to create a team row for Red team")

    sorted_cleaned_game_data = sorted(
        cleaned_game_data,
        key=lambda row: (row["player_name"] == "", row["player_name"], row["team_name"])
    )

    return sorted_cleaned_game_data


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

    # Get some Valornat metadata from the community resource valorant-api.com
    if log_to_terminal:
        print_log_to_terminal("Fetching map and agent metadata from valorant-api.com")
    maps_response = requests.get("https://valorant-api.com/v1/maps")
    maps_response = json.loads(maps_response.content)
    map_metadata = {}
    for map in maps_response["data"]:
        map_metadata[map["mapUrl"]] = map

    agents_response = requests.get("https://valorant-api.com/v1/agents")
    agents_response = json.loads(agents_response.content)
    agent_metadata = {}
    for agent in agents_response["data"]:
        agent_metadata[agent["uuid"]] = agent

    val_metadata = {
        "maps": map_metadata,
        "agents": agent_metadata
    }

    # Move forward with processing series
    output_array = []

    for series_id in SERIES_IDS_TO_PULL:
        print_log_to_terminal(f"Starting series {series_id}")
        # Get series info from Central Data
        query = SERIES_INFO_QUERY % series_id
        try:
            response = api.post(query)
            series_from_central_data = response["data"]["series"]
            series_metadata = {
                "series_id": series_id,
                "tournament_id": series_from_central_data["tournament"]["id"],
                "tournament_name": series_from_central_data["tournament"]["name"],
                "games": []
            }
        except Exception as error:
            print_log_to_terminal(f"Could not fetch metadata for series {series_id}: {str(error)}")
            continue

        # Get GRID end-state data
        grid_endstate_endpoint = "file-download/end-state/grid/series"
        try:
            grid_endstate_response = api.get(series_id, endpoint=grid_endstate_endpoint)
        except Exception as error:
            print_log_to_terminal(f"Could not fetch GRID end-state data for series {series_id}: {str(error)}")
            continue
        grid_series_endstate = json.loads(grid_endstate_response)

        for game_grid_endstate in grid_series_endstate["games"]:
            try:
                game_id = game_grid_endstate["id"]
                game_metadata = game_metadata_factory(game_grid_endstate)
                series_metadata["games"].append(game_metadata)
            except Exception as error:
                print_log_to_terminal(f"Could not parse game {game_id} from series {series_id}: {str(error)}")
            continue

        # Get Riot match history data
        response = api.get(series_id)
        zip_file = ZipFile(BytesIO(response))
        extracted_file = zip_file.open(zip_file.namelist()[0]).readlines()
        series_data = json.loads(extracted_file[0])
        if log_to_terminal:
            print_log_to_terminal(f"Series {series_id} contains {len(series_data)} games")

        for game_data in series_data:
            game_id = game_data["matchInfo"]["matchId"]
            if log_to_terminal:
                print_log_to_terminal(f"Sending game {game_id} to parser")
            cleaned_game_data = game_factory(game_data, series_metadata, val_metadata, log_to_terminal=log_to_terminal)

            # Append results into output_array
            output_array.extend(cleaned_game_data)

        print_log_to_terminal(f"Finished parsing {len(series_data)} games in series {series_id}")

    # Dump the output_array to a CSV and save to disk
    date_string = f"_{datetime.now().strftime('%Y%m%d_%H%M') if CONFIG['include_date_in_file_name'] else ''}"
    filename_to_use = f"{CONFIG['filename']}{date_string}.csv"
    with open(filename_to_use, "w", newline="") as file:
        csv_writer = csv.writer(file, delimiter=',')
        csv_writer.writerow([
            "game_id",
            "series_id",
            "tournament_id",
            "tournament_name",
            "map_id",
            "map_name",
            "game_start",
            "game_version",
            "game_number",
            "player_name",
            "team_id",
            "team_name",
            "agent_id",
            "agent_name",
            "win",
            "roundsWon",
            "roundsLost",
            "attackRoundsWon",
            "attackRoundsLost",
            "defenseRoundsWon",
            "defenseRoundsLost",
            "kills",
            "deaths",
            "assists",
            "averageCombatScore",
            "damagePerRound",
            "first_kills",
            "first_deaths",
            "headshot_rate"
        ])
        for row in output_array:
            csv_writer.writerow(row.values())

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
