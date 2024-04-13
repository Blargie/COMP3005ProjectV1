import psycopg
import getpass
import json
import os

#Global Variables
data_filter = {
    "La Liga" : ["2018/2019", "2019/2020", "2020/2021"],
    "Premier League" : ["2003/2004"]
}

data_filter_ids = {
    11 : [4, 42, 90],
    2 : [44]
}

#Lookup Table Population Functions
def populate_competitionstages():
    #Directory for Competition Stages: data/data/matches/2
    directories = ['json_loader\\data\\data\\matches\\2', 'json_loader\\data\\data\\matches\\11']

    # Iterate through each directory
    for directory in directories:
        #Iterate Through Each Match files (json files)
        for match_file in os.listdir(directory):
            match_path = os.path.join(directory, match_file)
            #Open JSON file
            with open(match_path, 'r', encoding='utf-8') as f:
                match_data = f.read()
            #Parse JSON file
            match_data = json.loads(match_data)
            #Get Cursor
            with conn.cursor() as cursor:
                #Iterate Through Each Match
                for match in match_data:
                    #Gather Match Information about the competition_sage id and name
                    competition_stage_id = match["competition_stage"]["id"]
                    competition_stage_name = match["competition_stage"]["name"]
                    #Execute Query to Add Competition Stage to Table. ID is a primary key so it will not add duplicates

                    cursor.execute("""
                        INSERT INTO competitionstages (CompetitionStageID, CompetitionStageName)
                        VALUES (%s, %s)
                        ON CONFLICT (CompetitionStageID) DO NOTHING
                    """, (competition_stage_id, competition_stage_name))
                    print("Added - Competition Stage: ", competition_stage_name)
            #Commit INSERT queries
    

def populate_positions():
    positions = {(None, 0), ("Goalkeeper", 1), ("Right Back", 2), ("Right Center Back", 3), ("Center Back", 4), ("Left Center Back", 5), ("Left Back", 6), ("Right Wing Back", 7), ("Left Wing Back", 8), ("Right Defensive Midfield", 9), ("Center Defensive Midfield", 10), ("Left Defensive Midfield", 11), ("Right Midfield", 12), ("Right Center Midfield", 13), ("Center Midfield", 14), ("Left Center Midfield", 15), ("Left Midfield", 16), ("Right Wing", 17), ("Right Attacking Midfield", 18), ("Center Attacking Midfield", 19), ("Left Attacking Midfield", 20), ("Left Wing", 21), ("Right Center Forward", 22), ("Striker", 23), ("Left Center Forward", 24), ("Secondary Striker", 25)}
    with conn.cursor() as cursor:
        for position in positions:
            cursor.execute("""
                INSERT INTO positions (positionname, positionid)
                VALUES (%s, %s)
                ON CONFLICT (positionid) DO NOTHING
            """, (position[0], position[1]))
            print("Added - Position: ", position[0])

def populate_event_types():
    event_types = {
        (42, "Ball Receipt"),
        (2, "Ball Recovery"),
        (3, "Dispossessed"),
        (4, "Duel"),
        (5, "Camera On"),
        (6, "Block"),
        (8, "Offside"),
        (9, "Clearance"),
        (10, "Interception"),
        (14, "Dribble"),
        (16, "Shot"),
        (17, "Pressure"),
        (18, "Half Start"),
        (19, "Substitution"),
        (20, "Own Goal Against"),
        (21, "Foul Won"),
        (22, "Foul Committed"),
        (23, "Goal Keeper"),
        (24, "Bad Behaviour"),
        (25, "Own Goal For"),
        (26, "Player On"),
        (27, "Player Off"),
        (28, "Shield"),
        (30, "Pass"),
        (33, "50/50"),
        (34, "Half End"),
        (35, "Starting XI"),
        (36, "Tactical Shift"),
        (37, "Error"),
        (38, "Miscontrol"),
        (39, "Dribbled Past"),
        (40, "Injury Stoppage"),
        (41, "Referee Ball-Drop"),
        (43, "Carry")
    }
    with conn.cursor() as cursor:
        for event in event_types:
            cursor.execute("""
                INSERT INTO eventtypes (eventtypeid, eventname)
                VALUES (%s, %s)
                ON CONFLICT (eventtypeid) DO NOTHING
            """, (event[0], event[1]))
            print("Added - Event Type: ", event[1])

def populate_play_patterns():
    play_patterns = {
        (1, "Regular Play"),
        (2, "From Corner"),
        (3, "From Free Kick"),
        (4, "From Throw In"),
        (5, "Other"),
        (6, "From Counter"),
        (7, "From Goal Kick"),
        (8, "From Keeper"),
        (9, "From Kick Off")}
    with conn.cursor() as cursor:
        for pattern in play_patterns:
            cursor.execute("""
                INSERT INTO playpatterns (playpatternid, playpatternname)
                VALUES (%s, %s)
                ON CONFLICT (playpatternid) DO NOTHING
            """, (pattern[0], pattern[1]))
            print("Added - Play Pattern: ", pattern[1])

def populate_fifty_fifty_outcomes():
    outcomes = {
        (1, "Lost"),
        (2, "Success To Opposition"),
        (3, "Success To Team"),
        (4, "Won")}
    with conn.cursor() as cursor:
        for outcome in outcomes:
            cursor.execute("""
                INSERT INTO fiftyfiftyoutcomes (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - 50/50 Outcome: ", outcome[1])

def populate_card():
    cards = {(7, "Yellow Card"), 
             (5, "Red Card"), 
             (6, "Second Yellow")}
    with conn.cursor() as cursor:
        for card in cards:
            cursor.execute("""
                INSERT INTO card (cardid, cardname)
                VALUES (%s, %s)
                ON CONFLICT (cardid) DO NOTHING
            """, (card[0], card[1]))
            print("Added - Card: ", card[1])

def populate_ball_receipt_outcomes():
    outcomes = {(9, "Incomplete")}
    with conn.cursor() as cursor:
        for outcome in outcomes:
            cursor.execute("""
                INSERT INTO ballreceiptoutcomes (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Ball Receipt Outcome: ", outcome[1])

def populate_body_part():
    body_parts = {(37, "Head"), (38, "Left Foot"), (40, "Right Foot"), (70, "Other"), (68, "Drop Kick"), (35, "Both Hands"), (36, "Chest"), (39, "Left Hand"), (41, "Right Hand"), (69, "Keeper Arm"), (106, "No Touch")}
    with conn.cursor() as cursor:
        for part in body_parts:
            cursor.execute("""
                INSERT INTO bodypart (bodypartid, bodypartname)
                VALUES (%s, %s)
                ON CONFLICT (bodypartid) DO NOTHING
            """, (part[0], part[1]))
            print("Added - Body Part: ", part[1])

def populate_dribble_outcomes():
    outcomes = {(8, "Complete"), (9, "Incomplete")}
    with conn.cursor() as cursor:
        for outcome in outcomes:
            cursor.execute("""
                INSERT INTO dribbleoutcomes (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Dribble Outcome: ", outcome[1])

def populate_duel_type():
    duel_type = {(10, "Aerial Lost"), (11, "Tackle")}
    with conn.cursor() as cursor:
        for duel in duel_type:
            cursor.execute("""
                INSERT INTO dueltype (duelid, duelname)
                VALUES (%s, %s)
                ON CONFLICT (duelid) DO NOTHING
            """, (duel[0], duel[1]))
            print("Added - Duel Type: ", duel[1])

def populate_duel_outcome():
    outcomes = {(1, "Lost"), (4, "Won"), (13, "Lost In Play"), (14, "Lost Out"), (15, "Success"), (16, "Success In Play"), (17, "Success Out")}
    with conn.cursor() as cursor:
        for outcome in outcomes:
            cursor.execute("""
                INSERT INTO dueloutcome (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Duel Outcome: ", outcome[1])

def populate_foul_type():
    foul_type = {(19, "6 Seconds"), (20, "Backpass Pick"), (21, "Dangerous Play"), (22, "Dive"), (23, "Foul Out"), (24, "Handball")}
    with conn.cursor() as cursor:
        for foul in foul_type:
            cursor.execute("""
                INSERT INTO foultype (foulid, foulname)
                VALUES (%s, %s)
                ON CONFLICT (foulid) DO NOTHING
            """, (foul[0], foul[1]))
            print("Added - Foul Type: ", foul[1])

def populate_goalkeeper_position():
    positions = {(42, "Moving"), (43, "Prone"), (44, "Set")}
    with conn.cursor() as cursor:
        for position in positions:
            cursor.execute("""
                INSERT INTO goalkeeperposition (positionid, positionname)
                VALUES (%s, %s)
                ON CONFLICT (positionid) DO NOTHING
            """, (position[0], position[1]))
            print("Added - Goalkeeper Position: ", position[1])

def populate_goalkeeper_technique():
    technique = {(45, "Diving"), (46, "Standing")}
    with conn.cursor() as cursor:
        for tech in technique:
            cursor.execute("""
                INSERT INTO goalkeepertechnique (techniqueid, techniquename)
                VALUES (%s, %s)
                ON CONFLICT (techniqueid) DO NOTHING
            """, (tech[0], tech[1]))
            print("Added - Goalkeeper Technique: ", tech[1])

def populate_goalkeeper_type():
    types = {(25, "Collected"), (26, "Goal Conceded"), (27, "Keeper Sweeper"), (28, "Penalty Conceded"), (29, "Penalty Saved"), (30, "Punched"), (31, "Saved"), (32, "Shot Faced"), (33, "Shot Saved"), (34, "Smother"), (113, "Shot Saved Off T"), (114, "Shot Saved To Post"), (110, "Saved To Post"), (109, "Penalty Saved To Post")}
    with conn.cursor() as cursor:
        for t in types:
            cursor.execute("""
                INSERT INTO goalkeepertype (typeid, typename)
                VALUES (%s, %s)
                ON CONFLICT (typeid) DO NOTHING
            """, (t[0], t[1]))
            print("Added - Goalkeeper Type: ", t[1])

def populate_goalkeeper_outcome():
    outcomes = {
        (47, "Claim"),
        (48, "Clear"),
        (49, "Collected Twice"),
        (50, "Fail"),
        (51, "In Play"),
        (52, "In Play Danger"),
        (53, "In Play Safe"),
        (55, "No Touch"),
        (56, "Saved Twice"),
        (15, "Success"),
        (58, "Touched In"),
        (59, "Touched Out"),
        (4, "Won"),
        (16, "Success In Play"),
        (17, "Success Out"),
        (13, "Lost In Play"),
        (14, "Lost Out"),
        (117, "Punched Out")
    } 
    with conn.cursor() as cursor:
        for outcome in outcomes:
            cursor.execute("""
                INSERT INTO goalkeeperoutcome (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Goalkeeper Outcome: ", outcome[1])

def populate_interception_outcome():
    interception_outcomes = {
        (1, "Lost"),
        (13, "Lost In Play"),
        (14, "Lost Out"),
        (15, "Success"),
        (16, "Success In Play"),
        (17, "Success Out"),
        (4, "Won")
    } 
    with conn.cursor() as cursor:
        for outcome in interception_outcomes:
            cursor.execute("""
                INSERT INTO interceptionoutcome (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Interception Outcome: ", outcome[1])

def populate_pass_height():
    pass_heights = {
        (1, "Ground Pass"),
        (2, "Low Pass"),
        (3, "High Pass")
    } 
    with conn.cursor() as cursor:
        for height in pass_heights:
            cursor.execute("""
                INSERT INTO passheight (heightid, heightname)
                VALUES (%s, %s)
                ON CONFLICT (heightid) DO NOTHING
            """, (height[0], height[1]))
            print("Added - Pass Height: ", height[1])

def populate_pass_type():
    pass_types = {
        (61, "Corner"),
        (62, "Free Kick"),
        (63, "Goal Kick"),
        (64, "Interception"),
        (65, "Kick Off"),
        (66, "Recovery"),
        (67, "Throw-in")
    } 
    with conn.cursor() as cursor:
        for pass_type in pass_types:
            cursor.execute("""
                INSERT INTO passtype (typeid, typename)
                VALUES (%s, %s)
                ON CONFLICT (typeid) DO NOTHING
            """, (pass_type[0], pass_type[1]))
            print("Added - Pass Type: ", pass_type[1])

def populate_pass_outcome():
    pass_outcomes = {
        (9, "Incomplete"),
        (74, "Injury Clearance"),
        (75, "Out"),
        (76, "Pass Offside"),
        (77, "Unknown")
    } 
    with conn.cursor() as cursor:
        for outcome in pass_outcomes:
            cursor.execute("""
                INSERT INTO passoutcome (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Pass Outcome: ", outcome[1])

def populate_pass_technique():
    pass_techniques = {
        (104, "Inswinging"),
        (105, "Outswinging"),
        (107, "Straight"),
        (108, "Through Ball")
    } 
    with conn.cursor() as cursor:
        for technique in pass_techniques:
            cursor.execute("""
                INSERT INTO passtechnique (techniqueid, techniquename)
                VALUES (%s, %s)
                ON CONFLICT (techniqueid) DO NOTHING
            """, (technique[0], technique[1]))
            print("Added - Pass Technique: ", technique[1])

def populate_shot_technique():
    shot_techniques = {
        (89, "Backheel"),
        (90, "Diving Header"),
        (91, "Half Volley"),
        (92, "Lob"),
        (93, "Normal"),
        (94, "Overhead Kick"),
        (95, "Volley")
    } 
    with conn.cursor() as cursor:
        for technique in shot_techniques:
            cursor.execute("""
                INSERT INTO shottechnique (techniqueid, techniquename)
                VALUES (%s, %s)
                ON CONFLICT (techniqueid) DO NOTHING
            """, (technique[0], technique[1]))
            print("Added - Shot Technique: ", technique[1])

def populate_shot_type():
    shot_types = {
        (61, "Corner"),
        (62, "Free Kick"),
        (87, "Open Play"),
        (88, "Penalty"),
        (65, "Kick Off")
    } 
    with conn.cursor() as cursor:
        for shot_type in shot_types:
            cursor.execute("""
                INSERT INTO shottype (typeid, typename)
                VALUES (%s, %s)
                ON CONFLICT (typeid) DO NOTHING
            """, (shot_type[0], shot_type[1]))
            print("Added - Shot Type: ", shot_type[1])

def populate_shot_outcome():
    shot_outcomes = {
        (96, "Blocked"),
        (97, "Goal"),
        (98, "Off T"),
        (99, "Post"),
        (100, "Saved"),
        (101, "Wayward"),
        (115, "Saved Off T"),
        (116, "Saved To Post")
    } 
    with conn.cursor() as cursor:
        for outcome in shot_outcomes:
            cursor.execute("""
                INSERT INTO shotoutcome (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (outcome[0], outcome[1]))
            print("Added - Shot Outcome: ", outcome[1])

def populate_substitution_outcome():
    outcome = {(102, "Injury"), (103, "Tacitcal")}
    with conn.cursor() as cursor:
        for out in outcome:
            cursor.execute("""
                INSERT INTO substitutionoutcome (outcomeid, outcomename)
                VALUES (%s, %s)
                ON CONFLICT (outcomeid) DO NOTHING
            """, (out[0], out[1]))
            print("Added - Substitution Outcome: ", out[1])

def populate_lookup():
    populate_competitionstages()
    populate_positions()
    populate_event_types()
    populate_play_patterns()
    populate_fifty_fifty_outcomes()
    populate_card()
    populate_ball_receipt_outcomes()
    populate_body_part()
    populate_dribble_outcomes()
    populate_duel_type()
    populate_duel_outcome()
    populate_foul_type()
    populate_goalkeeper_position()
    populate_goalkeeper_technique()
    populate_goalkeeper_type()
    populate_goalkeeper_outcome()
    populate_interception_outcome()
    populate_pass_height()
    populate_pass_type()
    populate_pass_outcome()
    populate_pass_technique()
    populate_shot_technique()
    populate_shot_type()
    populate_shot_outcome()
    populate_substitution_outcome()

#Main Table Popuation Functions
def populate_competitions(competition_filter):
    #Directory For Competitions
    directory = 'json_loader\\data\\data\\competitions.json'

    #Open JSON file
    with open(directory, 'r') as f:
        competition_file = f.read()

    #Parse the JSON file
    competition_data = json.loads(competition_file)
    

    #Get Cursor
    with conn.cursor() as cursor:
        #Iterate Through File
        for competition in competition_data:
            #Gather Competition Information
            competition_id = competition["competition_id"]
            season_id = competition["season_id"]
            country_name = competition["country_name"]
            competition_name = competition["competition_name"]
            competition_gender = competition["competition_gender"]
            competition_youth = competition["competition_youth"]
            competition_international = competition["competition_international"]
            season_name = competition["season_name"]
            match_updated = competition["match_updated"]
            match_updated_360 = competition["match_updated_360"]
            match_available = competition["match_available"]
            match_available_360 = competition["match_available_360"]

            #Check if it is a competition we're interested in
            if competition_name in competition_filter:
                season_filter = competition_filter[competition_name]
                if season_name in season_filter:
                    print("Added - Competition: ", competition_name, " Season: ", season_name)
                    #Execute Query to Add Competition to Table
                    with conn.cursor() as cursor:
                        cursor.execute("""
            INSERT INTO competitions (CompetitionID, SeasonID, CountryName, CompetitionName, CompetitionGender, CompetitionYouth, CompetitionInternational, SeasonName, MatchUpdated, MatchUpdated360, MatchAvailable, MatchAvailable360)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (competition_id, season_id, country_name, competition_name, competition_gender, competition_youth, competition_international, season_name, match_updated, match_updated_360, match_available, match_available_360))          
    #Commit Queries

def populate_matches (competition_filter):
    #Directory for Match Folders
    directory = 'json_loader\\data\\data\\matches'

    #Get Cursor
    with conn.cursor() as cursor:
    #Iterate Through Matches by Competition
        for competition_id in os.listdir(directory):
            #Check if CompetitionID is a valid CompetitionID
            if int(competition_id) in competition_filter.keys():
                print("Searching Matches under Competition: ", int(competition_id))
                #Join Filepath for Competition
                competition_path = os.path.join(directory, competition_id)
                #Iterate Through Matches by Season
                for season_id in os.listdir(competition_path):
                    #Check if SeasonID is a valid SeasonID
                    trimmed_season_id = season_id.replace('.json', "")
                    if int(trimmed_season_id) in competition_filter[int(competition_id)]:
                        print("Searching Matches under Season: ", int(trimmed_season_id))
                        #Join Filepath for Season
                        season_path = os.path.join(competition_path, season_id)
                        #Read the JSON file
                        with open(season_path, 'r', encoding='utf-8') as f:
                            try:
                                match_file = f.read()
                            except json.JSONDecodeError:
                                print("ERROR: Failed to decode JSON file ", season_id)
                            except Exception as e:
                                print(e)
                                exit(1)
                        #Parse the JSON file
                        match_data = json.loads(match_file)

                        #Iterate Through the Match Data
                        for match in match_data:
                            #Get all of the Match info
                            match_id = match['match_id']
                            match_date = match['match_date']
                            kick_off = match['kick_off']
                            competition_id = match['competition']['competition_id']
                            season_id = match['season']['season_id']
                            home_score = match['home_score']
                            away_score = match['away_score']
                            match_status = match['match_status']
                            match_status_360 = match['match_status_360']
                            last_updated = match['last_updated']
                            last_updated_360 = match['last_updated_360']
                            match_week = match['match_week']
                            competition_stage = match['competition_stage']['id']


                            #Get Country info for populating Country table
                            #Get Country Data
                            home_team_country = match['home_team']['country']
                            away_team_country = match['away_team']['country']
                            home_country_id = home_team_country['id']
                            home_country_name = home_team_country['name']
                            away_country_id = away_team_country['id']
                            away_country_name = away_team_country['name']
                            #Populate the Country table
                            #Check if Home Country Exists
                            cursor.execute("SELECT 1 FROM country WHERE CountryID = %s", (home_country_id,))
                            if not cursor.fetchone():
                                #Print Added Country
                                print("Added - CountryID: ", home_country_id, " CountryName: ", home_country_name)
                                cursor.execute("""INSERT INTO country (CountryID, CountryName) 
                                                  VALUES (%s, %s)""", (home_country_id, home_country_name))
                            #Check if Away Country Exists
                            cursor.execute("SELECT 1 FROM country WHERE CountryID = %s", (away_country_id,))
                            if not cursor.fetchone():
                                #Print Added Country
                                print("Added - CountryID: ", home_country_id, " CountryName: ", home_country_name)
                                cursor.execute("""INSERT INTO country (CountryID, CountryName) 
                                                  VALUES (%s, %s)""", (away_country_id, away_country_name))

                            #Get Team info for populating Teams table
                            home_team = match['home_team']
                            away_team = match['away_team']
                            #Populate Teams table
                            #Get Home Team Info
                            home_team_id = home_team['home_team_id']
                            home_team_name = home_team['home_team_name']
                            home_team_gender = home_team['home_team_gender']
                            home_team_group = home_team['home_team_group']
                            
                            #Get Away Team Info
                            away_team_id = away_team['away_team_id']
                            away_team_name = away_team['away_team_name']
                            away_team_gender = away_team['away_team_gender']
                            away_team_group = away_team['away_team_group']
                            
                        
                            #Populate the Teams Table
                            #Check if Home Team exists
                            cursor.execute("SELECT 1 FROM teams WHERE TeamID = %s", (home_team_id,))
                            if not cursor.fetchone():
                                #Print Added Team]
                                print("Added - TeamID: ", home_team_id, " TeamName: ", home_team_name)
                                #Add Team to Teams table
                                cursor.execute("""INSERT INTO teams (TeamID, TeamName, TeamGender, TeamGroup, CountryID)
                                                  VALUES (%s, %s, %s, %s, %s)""", (home_team_id, home_team_name, home_team_gender, home_team_group, home_country_id))
                            #Check if Away Team exists
                            cursor.execute("SELECT 1 FROM teams WHERE TeamID = %s", (away_team_id,))
                            if not cursor.fetchone():
                                #Print Added Team]
                                print("Added - TeamID: ", home_team_id, " TeamName: ", home_team_name)
                                # Add Team to Teams table
                                cursor.execute("""INSERT INTO teams (TeamID, TeamName, TeamGender, TeamGroup, CountryID)
                                                  VALUES (%s, %s, %s, %s, %s)""", (away_team_id, away_team_name, away_team_gender, away_team_group, away_country_id))
                            #Populate Managers Table
                            if "manager" in away_team.keys():
                                for manager in away_team['managers']:
                                    #Get Manager Info
                                    manager_id = manager['id']
                                    manager_name = manager['name']
                                    manager_nickname = manager['nickname']
                                    manager_dob = manager['dob']
                                    manager_country = manager['country']['id']
                                    manager_country_name = manager['country']['name']
                                    #Check if Country Exists
                                    cursor.execute("""SELECT 1 FROM country WHERE CountryID = %s""", (manager_country,))
                                    if not cursor.fetchone():
                                        #Print Added Country
                                        print("Added - CountryID: ", manager_country, " CountryName: ", manager_country_name)
                                        #Add Couuntry
                                        cursor.execute("""INSERT INTO country (CountryID, CountryName)
                                                        VALUES (%s, %s)""", (manager_country, manager_country_name))
                                    #Check Manager exists
                                    cursor.execute("""SELECT 1 FROM Managers WHERE ManagerID = %s""", (manager_id,))
                                    if not cursor.fetchone():
                                        #Print Added Manager
                                        print("Added - ManagerID: ", manager_id, " ManagerName: ", manager_name)
                                        #Add Manager to Managers Table
                                        cursor.execute("""INSERT INTO Managers (ManagerID, ManagerName, Nickname, DateOfBirth, CountryID)
                                                        VALUES (%s, %s, %s, %s, %s)""", (manager_id, manager_name, manager_nickname, manager_dob, manager_country))
                                        #Add Manager to TeamManagers table
                                        cursor.execute("""INSERT INTO TeamManagers (TeamID, ManagerID)
                                                        VALUES (%s, %s)""", (home_team_id, manager_id))
                            if "manager" in home_team.keys():
                                for manager in home_team['managers']:
                                    #Get Manager Info
                                    manager_id = manager['id']
                                    manager_name = manager['name']
                                    manager_nickname = manager['nickname']
                                    manager_dob = manager['dob']
                                    manager_country = manager['country']['id']
                                    manager_country_name = manager['country']['name']
                                    #Check if Country Exists
                                    cursor.execute("""SELECT 1 FROM country WHERE CountryID = %s""", (manager_country,))
                                    if not cursor.fetchone():
                                        #Print Added Country
                                        print("Added - CountryID: ", manager_country, " CountryName: ", manager_country_name)
                                        #Add Couuntry
                                        cursor.execute("""INSERT INTO country (CountryID, CountryName)
                                                        VALUES (%s, %s)""", (manager_country, manager_country_name))
                                    #Check Manager exists
                                    cursor.execute("""SELECT 1 FROM Managers WHERE ManagerID = %s""", (manager_id,))
                                    if not cursor.fetchone():
                                        #Print Added Manager
                                        print("Added - ManagerID: ", manager_id, " ManagerName: ", manager_name)
                                        #Add Manager to Managers Table
                                        cursor.execute("""INSERT INTO Managers (ManagerID, ManagerName, Nickname, DateOfBirth, CountryID)
                                                        VALUES (%s, %s, %s, %s, %s)""", (manager_id, manager_name, manager_nickname, manager_dob, manager_country))
                                        #Add Manager to TeamManagers table
                                        cursor.execute("""INSERT INTO TeamManagers (TeamID, ManagerID)
                                                        VALUES (%s, %s)""", (home_team_id, manager_id))

                            #Get Metadata info for populating Metadata table
                            metadata = match['metadata']
                            data_version = metadata['data_version']
                            shot_fidelity_version = metadata['shot_fidelity_version']
                            if "xy_fidelity_version" in metadata.keys():
                                xy_fidelity_version = metadata['xy_fidelity_version']
                            else:
                                xy_fidelity_version = None
                            #Get Stadium info for populating Stadiums table
                            if "stadium" in match.keys():
                                stadium = match['stadium']
                                stadium_id = stadium['id']
                                stadium_name = stadium['name']
                                stadium_country = stadium['country']['id']
                                #Check if a Stadium Exists
                                cursor.execute("SELECT 1 FROM stadiums WHERE StadiumID = %s", (stadium_id,))
                                if not cursor.fetchone():
                                    #Print Added Stadium
                                    print("Added - StadiumID: ", stadium_id, " StadiumName: ", stadium_name)
                                    #Add Stadium to Stadiums
                                    cursor.execute("""INSERT INTO stadiums (StadiumID, StadiumName, CountryID) 
                                                    VALUES (%s, %s, %s)""", (stadium_id, stadium_name, stadium_country))
                            else:
                                stadium = None
                            #Get Referee info for populating Referees table
                            if "referee" in match.keys():
                                referee = match['referee']
                                referee_id = referee['id']
                                referee_name = referee['name']
                                referee_country = referee['country']['id']
                                #Check if a Referee Exists
                                cursor.execute("SELECT 1 FROM referees WHERE RefereeID = %s", (referee_id,))
                                if not cursor.fetchone():
                                    #Print Added Referee
                                    print("Added - RefereeID: ", referee_id, " RefereeName: ", referee_name)
                                    #Add Referee to Referees
                                    cursor.execute("""INSERT INTO referees (RefereeID, RefereeName, CountryID)
                                                    VALUES (%s, %s, %s)""", (referee_id, referee_name, referee_country))
                            else:
                                refere = None

                            #Populate Tables
                            print("Added - Match: ", match_id)
                            #Add Match to Matches
                            cursor.execute("""INSERT INTO matches (MatchID, MatchDate, KickOff, CompetitionID, SeasonID, HomeTeam, AwayTeam, HomeScore, AwayScore, MatchStatus, MatchStatus360, LastUpdated, LastUpdated360, MatchWeek, CompetitionStage, Stadium, Referee)
                                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (match_id, match_date, kick_off, competition_id, season_id, home_team_id, away_team_id, home_score, away_score, match_status, match_status_360, last_updated, last_updated_360, match_week, competition_stage, stadium_id, referee_id))
                            #Add Metadata to Metadata
                            cursor.execute("""INSERT INTO metadata (MatchID, DataVersion, ShotFidelityVersion, XYFidelityVersion)
                                              VALUES (%s, %s, %s, %s)""", (match_id, data_version, shot_fidelity_version, xy_fidelity_version))
    #Commit Queries

def populate_lineups (competition_filter):
    #Directory for the Lineups
    directory = 'json_loader\\data\\data\\lineups'
    #Get Cursor
    with conn.cursor() as cursor:
        #Iterate Through Each Match's lineups (json files)
        for match_id in os.listdir(directory):
            #Trim
            trimmed_match_id = int(match_id.replace('.json', ""))
            #Check if lineup corresponds to a match in database
            cursor.execute("SELECT 1 FROM matches WHERE MatchID = %s", (trimmed_match_id,))
            if cursor.fetchone():
                #Match is valid, begin iterating through file
                lineup_path = os.path.join(directory, match_id)
                with open(lineup_path, 'r', encoding='utf-8') as f:
                    try:
                        lineups_file = f.read()
                    except json.JSONDecodeError:
                        print("ERROR: Failed to decode JSON file ", match_id)
                    except Exception as e:
                        print(e)
                        exit(1)
                #Parse the JSON file
                lineups_data = json.loads(lineups_file)

                #Iterate Through Lineups in Match
                for lineup in lineups_data:
                    lineup_team_id = lineup['team_id']
                    lineup_team_name = lineup['team_name']
                    #Create Lineup
                    print("Added - Lineup MatchID: ", trimmed_match_id, " Lineup Team: ", lineup_team_name)
                    cursor.execute("""INSERT INTO lineups (MatchID, TeamID)
                                      VALUES (%s, %s)""", (trimmed_match_id, lineup_team_id))
                    #Iterate through players in lineup
                    players = lineup['lineup']
                    for player in players:
                        #Get player info
                        player_id = player['player_id']
                        player_name = player['player_name']
                        player_nickname = player['player_nickname']
                        player_lineup_jersey_number = player['jersey_number']
                        player_lineup_country_id = player['country']['id']
                        player_lineup_country_name = player['country']['name']

                        #Check if Country exists
                        cursor.execute("SELECT 1 FROM country WHERE CountryID = %s", (player_lineup_country_id,))
                        if not cursor.fetchone():
                            #Create Country
                            print("Added - CountryID: ", player_lineup_country_id, " CountryName: ", player_lineup_country_name)
                            cursor.execute("""INSERT INTO country (CountryID, CountryName)
                                              VALUES (%s, %s)""", (player_lineup_country_id, player_lineup_country_name))

                        #Create Player
                        #Check if Player exists
                        cursor.execute("SELECT 1 FROM players WHERE PlayerID = %s", (player_id,))
                        if not cursor.fetchone():
                            #Create Player
                            print("Added - PlayerID: ", player_id, " PlayerName: ", player_name)
                            cursor.execute("""INSERT INTO players (PlayerID, PlayerName, Nickname)
                                              VALUES (%s, %s, %s)""", (player_id, player_name, player_nickname))
                        #Create LineupPlayer
                        cursor.execute("""INSERT INTO lineupplayers (MatchID, TeamID, PlayerID, JerseyNumber, CountryID)
                                          VALUES (%s, %s, %s, %s, %s)""", (trimmed_match_id, lineup_team_id, player_id, player_lineup_jersey_number, player_lineup_country_id))

                        #Iterate through cards
                        for card in player['cards']:
                            #Get Card Info
                            card_time = card['time']
                            card_type = card['card_type']
                            card_reason = card['reason']
                            card_period = card['period']
                            #Create LineupPlayerCard
                            cursor.execute("""INSERT INTO lineupplayercard (MatchID, PlayerID, CardTime, CardType, Reason, Period)
                                              VALUES (%s, %s, %s, %s, %s, %s)""", (trimmed_match_id, player_id, card_time, card_type, card_reason, card_period))

                        #Create Player Position
                        positions = player['positions']
                        #Iterate Through Positions
                        for position in positions:
                            #Get Position Info
                            position_id = position['position_id']
                            position_from = position['from']
                            position_to = position['to']
                            position_from_period = position['from_period']
                            position_to_period = position['to_period']
                            position_start_reason = position['start_reason']
                            position_end_reason = position['end_reason']

                            #Create Position
                            cursor.execute("""INSERT INTO playerpositions (MatchID, PlayerID, PositionID, SwitchedFrom, SwitchedTo, FromPeriod, ToPeriod, StartReason, EndReason)
                                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", (trimmed_match_id, player_id, position_id, position_from, position_to, position_from_period, position_to_period, position_start_reason, position_end_reason))
    #Commit Queries


def populate_events (competition_filter):
    #Directory for Events
    directory = 'json_loader\\data\\data\\events'
    #Get Cursor
    with conn.cursor() as cursor:
        #Iterate Through Events Folder
        for match_id in os.listdir(directory):
            #Trim 
            trimmed_match_id = int(match_id.replace('.json', ""))
            #Check if event corresponds to a match in database
            cursor.execute("SELECT 1 FROM matches WHERE MatchID = %s", (trimmed_match_id,))
            if cursor.fetchone():
                #Match is valid, begin iterating through events file
                events_path = os.path.join(directory, match_id)
                with open(events_path, 'r', encoding='utf-8') as f:
                    try:
                        events_file = f.read()
                    except json.JSONDecodeError:
                        print("ERROR: Failed to decode JSON file ", match_id)
                    except Exception as e:
                        print(e)
                        exit(1)
                #Parse the JSON file
                events_data = json.loads(events_file)

                #Iterate Through Events in Match
                for event in events_data:
                    event_id = event['id']
                    event_index = event['index']
                    event_period = event['period']
                    event_timestamp = event['timestamp']
                    event_minute = event['minute']
                    event_second = event['second']
                    event_type = event['type']['id']
                    event_name = event['type']['name']
                    event_possession = event['possession']
                    event_possession_team = event['possession_team']['id']
                    event_play_pattern = event['play_pattern']['id']
                    event_team = event['team']['id']
                    #Event Player
                    if 'player' in event.keys():
                        event_player = event['player']['id']
                    else:
                        event_player = None
                    #Event Position
                    if 'position' in event.keys():
                        event_position = event['position']['id']
                    else:
                        event_position = None
                    #Event Location
                    if 'location' in event.keys():
                        event_location_x = event['location'][0]
                        event_location_y = event['location'][1]
                        event_location_z = event['location'][2] if len(event['location']) > 2 else None
                    else:
                        event_location_x = None
                        event_location_y = None
                        event_location_z = None
                    #Event Duration
                    if 'duration' in event.keys():
                        event_duration = event['duration']
                    else:
                        event_duration = None
                    #Event Pressure
                    if 'under_pressure' in event.keys():
                        event_under_pressure = event['under_pressure']
                    else:
                        event_under_pressure = None
                    #Event OffCamera
                    if 'off_camera' in event.keys():
                        event_off_camera = event['off_camera']
                    else:
                        event_off_camera = None
                    #Event Out
                    if 'out' in event.keys():
                        event_out = event['out']
                    else:
                        event_out = None
                    #Related Events
                    if 'related_events' in event.keys():
                        event_related_events = []
                        #Iterate for each related event
                        for related_event in event['related_events']:
                            event_related_events.append(related_event)
                    else:
                        event_related_events = None

                    #Print Event Added
                    print("Added - MatchID:", trimmed_match_id, "Event Name: ", event_name)

                    #Insert Event into Events table ToDo
                    cursor.execute("""INSERT INTO events (EventID, MatchID, Index, Period, Timestamp, Minute, Second, EventType, Possession, PossessionTeam, PlayPattern, TeamID, PlayerID, Position, LocationX, LocationY, LocationZ, Duration, UnderPressure, OffCamera, Out, RelatedEvents)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                                      (event_id, trimmed_match_id, event_index, event_period, event_timestamp, event_minute, event_second, event_type, event_possession, event_possession_team, event_play_pattern, event_team, event_player, event_position, event_location_x, event_location_y, event_location_z, event_duration, event_under_pressure, event_off_camera, event_out, event_related_events))

                    #Handle Event Object Tables
                    populate_event_object(event_type, event)
    #Commit Queries
    conn.commit()
    
def populate_event_object(event_type, event):
    #Will handle the INSERT queries for different event objects, ie. pass, shot, etc.
    match event_type:
        case 33:
            populate_fifty_fifty(event)
        case 24:
            populate_bad_behaviour(event)
        case 42:
            populate_ball_receipt(event)
        case 2:
            populate_ball_recovery(event)
        case 6:
            populate_block(event)
        case 43:
            populate_carry(event)
        case 9:
            populate_clearance(event)
        case 14:
            populate_dribble(event)
        case 39:
            populate_dribbled_past(event)
        case 4:
            populate_duel(event)
        case 22:
            populate_foulcommitted(event)
        case 21:
            populate_foulwon(event)
        case 23:
            populate_goalkeeper(event)
        case 34:
            populate_half_end(event)
        case 18:
            populate_half_start(event)
        case 40:
            populate_injury_stoppage(event)
        case 10:
            populate_interception(event)
        case 38:
            populate_miscontrol(event)
        case 16:
            populate_shot(event)
            populate_shot_freeze_frame(event)
        case 30:
            populate_pass(event)
        case 27:
            populate_player_off(event)
        case 17:
            populate_pressure(event)
        case 19:
            populate_substitution(event)

def populate_360(competition_filter):
    #Directory for 360
    directory = 'json_loader\\data\\data\\three-sixty'

    #Get Cursor
    with conn.cursor() as cursor:
        #Iterate Through 360 Folder
        for match_id in os.listdir(directory):
            #Trim
            trimmed_match_id = int(match_id.replace('.json', ""))
            #Check if 360 corresponds to a event in database
            cursor.execute("SELECT 1 FROM matches WHERE MatchID = %s", (trimmed_match_id,))
            if cursor.fetchone():
                #Event is valid, begin iterating through 360 file
                three_sixty_path = os.path.join(directory, match_id)
                with open(three_sixty_path, 'r', encoding='utf-8') as f:
                    try:
                        three_sixty_file = f.read()
                    except json.JSONDecodeError:
                        print("ERROR: Failed to decode JSON file ", match_id)
                    except Exception as e:
                        print(e)
                        exit(1)
                #Parse the JSON file
                three_sixty_data = json.loads(three_sixty_file)

                #Iterate Through 360 in Match
                for three_sixty_event in three_sixty_data:
                    three_sixty_event_id = three_sixty_event['event_uuid']
                    three_sixty_visible_area = three_sixty_event['visible_area']

                    #Print 360 Event Added
                    print("Added - 360 Event: ", three_sixty_event_id)
                    #Create 360 Event
                    cursor.execute("""INSERT INTO events360 (Event360ID, VisibleArea) VALUES (%s, %s)""", (three_sixty_event_id, three_sixty_visible_area))
                    
                    #Iterate through each Freeze Frame
                    for freeze_frame in three_sixty_event['freeze_frame']:
                        freeze_frame_teammate = freeze_frame['teammate']
                        freeze_frame_actor = freeze_frame['actor']
                        freeze_frame_keeper = freeze_frame['keeper']
                        freeze_frame_location_x = freeze_frame['location'][0]
                        freeze_frame_location_y = freeze_frame['location'][1]
                        freeze_frame_location_z = freeze_frame['location'][2] if len(freeze_frame['location']) > 2 else None

                        #Create Freeze Frame
                        cursor.execute(""""INSERT INTO event360freezeframes (Event360ID, Teammate, Actor, Keeper, LocationX, LocationY, LocationZ)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s)""", (three_sixty_event_id, freeze_frame_teammate, freeze_frame_actor, freeze_frame_keeper, freeze_frame_location_x, freeze_frame_location_y, freeze_frame_location_z))

#Event Object Population Functions   
def populate_fifty_fifty(event):
    if 'fifty_fifty' in event.keys():
        fifty_fifty = event['fifty_fifty']
        if "outcome" in fifty_fifty.keys():
            outcome = fifty_fifty['outcome']['id']
        else:
            outcome = None
        if "counterpress" in fifty_fifty.keys():
            counterpress = fifty_fifty['counterpress']
        else:
            counterpress = None
    else:
        outcome = None
        counterpress = None
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO fiftyfifty (eventid, outcome, counterpress)
                          VALUES (%s, %s, %s)""", (event['id'], outcome, counterpress))

def populate_bad_behaviour(event):
    if 'bad_behaviour' in event.keys():
        bad_behaviour = event['bad_behaviour']
        if "card" in bad_behaviour.keys():
            card = bad_behaviour['card']['id']
        else:
            card = None
    else:
        card = None
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO badbehaviour (eventid, cardid)
                          VALUES (%s, %s)""", (event['id'], card))

def populate_ball_receipt(event):
    if 'ball_receipt' in event.keys():
        ball_receipt = event['ball_receipt']
        if "outcome" in ball_receipt.keys():
            outcome = ball_receipt['outcome']['id']
        else:
            outcome = None
    else:
        outcome = None
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO ballreceipt (eventid, outcome)
                          VALUES (%s, %s)""", (event['id'], outcome))

def populate_ball_recovery(event):
    if 'ball_recovery' in event.keys():
        ball_recovery = event['ball_recovery']
        if "offensive" in ball_recovery.keys():
            offensive = ball_recovery['offensive']
        else:
            offensive = None
        if 'recovery_failure' in ball_recovery.keys():
            recovery_failure = ball_recovery['recovery_failure']
        else:
            recovery_failure = None
    else:
        offensive = None
        recovery_failure = None

    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO ballrecovery (eventid, offensive, recoveryfailure)
                          VALUES (%s, %s, %s)""", (event['id'], offensive, recovery_failure))

def populate_block(event):
    if 'block' in event.keys():
        block = event['block']
        if "deflection" in block:
            deflection = block['deflection']
        else:
            deflection = None

        if "offensive" in block:
            offensive = block['offensive']
        else:
            offensive = None

        if "saveBlock" in block:
            saveBlock = block['saveBlock']
        else:
            saveBlock = None

        if "counterpress" in block:
            counterpress = block['counterpress']
        else:
            counterpress = None
    else:
        deflection = None
        offensive = None
        saveBlock = None
        counterpress = None
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO block (eventid, deflection, offensive, saveblock, counterpress)
                          VALUES (%s, %s, %s, %s, %s)""", (event['id'], deflection, offensive, saveBlock, counterpress))

def populate_carry(event):
    if 'carry' in event.keys():
        carry = event['carry']
        if "end_location" in carry:
            end_location = carry['end_location']
            end_location_x = end_location[0]
            end_location_y = end_location[1]
            end_location_z = end_location[2] if len(end_location) > 2 else None
        else:
            end_location_x = None
            end_location_y = None
            end_location_z = None
    else:
        end_location_x = None
        end_location_y = None
        end_location_z = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO carry (eventid, endlocationx, endlocationy, endlocationz)
            VALUES (%s, %s, %s, %s)
        """, (event['id'], end_location_x, end_location_y, end_location_z))

def populate_clearance(event):
    if 'clearance' in event.keys():
        clearance = event['clearance']
        if "aerial_won" in clearance:
            aerial_won = clearance['aerial_won']
        else:
            aerial_won = None

        if "body_part" in clearance:
            body_part = clearance['body_part']['id']
        else:
            body_part = None
    else:
        aerial_won = None
        body_part = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO clearance (eventid, aerialwon, bodypart)
            VALUES (%s, %s, %s)
        """, (event['id'], aerial_won, body_part))

def populate_dribble(event):
    if 'dribble' in event.keys():
        dribble = event['dribble']
        if "overrun" in dribble:
            overrun = dribble['overrun']
        else:
            overrun = None

        if "nutmeg" in dribble:
            nutmeg = dribble['nutmeg']
        else:
            nutmeg = None

        if "outcome" in dribble:
            outcome = dribble['outcome']['id']
        else:
            outcome = None

        if "no_touch" in dribble:
            no_touch = dribble['no_touch']
        else:
            no_touch = None
    else:
        overrun = None
        nutmeg = None
        outcome = None
        no_touch = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO dribble (eventid, overrun, nutmeg, outcome, notouch)
            VALUES (%s, %s, %s, %s, %s)
        """, (event['id'], overrun, nutmeg, outcome, no_touch))

def populate_dribbled_past(event):
    if 'dribbled_past' in event.keys():
        dribbled_past = event['dribbled_past']
        if "counterpress" in dribbled_past:
            counterpress = dribbled_past['counterpress']
        else:
            counterpress = None
    else:
        counterpress = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO dribbledpast (eventid, counterpress)
            VALUES (%s, %s)
        """, (event['id'], counterpress))

def populate_duel(event):
    if 'duel' in event.keys():
        duel = event['duel']
        if "counterpress" in duel:
            counterpress = duel['counterpress']
        else:
            counterpress = None

        if "duel_type" in duel:
            duel_type = duel['duel_type']['id']
        else:
            duel_type = None

        if "outcome" in duel:
            outcome = duel['outcome']['id']
        else:
            outcome = None
    else:
        counterpress = None
        duel_type = None
        outcome = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO duel (eventid, counterpress, dueltype, outcome)
            VALUES (%s, %s, %s, %s)
        """, (event['id'], counterpress, duel_type, outcome))

def populate_foulcommitted(event):
    if 'foul_committed' in event.keys():
        foul_committed = event['foul_committed']
        if "counterpress" in foul_committed:
            counterpress = foul_committed['counterpress']
        else:
            counterpress = None

        if "offensive" in foul_committed:
            offensive = foul_committed['offensive']
        else:
            offensive = None

        if "foul_type" in foul_committed:
            foul_type = foul_committed['foul_type']['id']
        else:
            foul_type = None

        if "advantage" in foul_committed:
            advantage = foul_committed['advantage']
        else:
            advantage = None

        if "penalty" in foul_committed:
            penalty = foul_committed['penalty']
        else:
            penalty = None

        if "card" in foul_committed:
            card = foul_committed['card']['id']
        else:
            card = None
    else:
        counterpress = None
        offensive = None
        foul_type = None
        advantage = None
        penalty = None
        card = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO foulcommitted (eventid, counterpress, offensive, foultype, advantage, penalty, card)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (event['id'], counterpress, offensive, foul_type, advantage, penalty, card))

def populate_foulwon(event):
    if 'foul_won' in event.keys():
        foul_won = event['foul_won']
        if "defensive" in foul_won:
            defensive = foul_won['defensive']
        else:
            defensive = None

        if "advantage" in foul_won:
            advantage = foul_won['advantage']
        else:
            advantage = None

        if "penalty" in foul_won:
            penalty = foul_won['penalty']
        else:
            penalty = None
    else:
        defensive = None
        advantage = None
        penalty = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO foulwon (eventid, defensive, advantage, penalty)
            VALUES (%s, %s, %s, %s)
        """, (event['id'], defensive, advantage, penalty))

def populate_goalkeeper(event):
    if 'goalkeeper' in event.keys():
        goalkeeper = event['goalkeeper']
        if "position" in goalkeeper:
            position = goalkeeper['position']['id']
        else:
            position = None

        if "technique" in goalkeeper:
            technique = goalkeeper['technique']['id']
        else:
            technique = None

        if "body_part" in goalkeeper:
            body_part = goalkeeper['body_part']['id']
        else:
            body_part = None

        if "type" in goalkeeper:
            goalkeeper_type = goalkeeper['type']['id']
        else:
            goalkeeper_type = None

        if "outcome" in goalkeeper:
            outcome = goalkeeper['outcome']['id']
        else:
            outcome = None
    else:
        position = None
        technique = None
        body_part = None
        goalkeeper_type = None
        outcome = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO goalkeeper (eventid, position, technique, bodypart, goalkeepertype, outcome)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (event['id'], position, technique, body_part, goalkeeper_type, outcome))

def populate_half_end(event):
    if 'half_end' in event.keys():   
        half_end = event['half_end']
        if "early_video_end" in half_end:
            early_video_end = half_end['early_video_end']
        else:
            early_video_end = None

        if "match_suspended" in half_end:
            match_suspended = half_end['match_suspended']
        else:
            match_suspended = None
    else:
        early_video_end = None
        match_suspended = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO halfend (eventid, earlyvideoend, matchsuspended)
            VALUES (%s, %s, %s)
        """, (event['id'], early_video_end, match_suspended))

def populate_half_start(event):
    if 'half_start' in event.keys():
        half_start = event['half_start']
        if "late_video_start" in half_start:
            late_video_start = half_start['late_video_start']
        else:
            late_video_start = None
    else:
        late_video_start = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO halfstart (eventid, latevideostart)
            VALUES (%s, %s)
        """, (event['id'], late_video_start))

def populate_injury_stoppage(event):
    if 'injury_stoppage' in event.keys():
        injury_stoppage = event['injury_stoppage']
        if "in_chain" in injury_stoppage:
            in_chain = injury_stoppage['in_chain']
        else:
            in_chain = None
    else:
        in_chain = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO injurystoppage (eventid, inchain)
            VALUES (%s, %s)
        """, (event['id'], in_chain))

def populate_interception(event):
    if 'interception' in event.keys():
        interception = event['interception']
        if "outcome" in interception:
            outcome = interception['outcome']['id']
        else:
            outcome = None
    else:
        outcome = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO interception (eventid, outcome)
            VALUES (%s, %s)
        """, (event['id'], outcome))

def populate_miscontrol(event):
    if 'miscontrol' in event.keys():
        miscontrol = event['miscontrol']
        if "aerial_won" in miscontrol:
            aerial_won = miscontrol['aerial_won']
        else:
            aerial_won = None
    else:
        aerial_won = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO miscontrol (eventid, aerialwon)
            VALUES (%s, %s)
        """, (event['id'], aerial_won))

def populate_shot(event):
    if 'shot' in event.keys():
        shot = event['shot']
        key_pass_id = shot['key_pass_id'] if "key_pass_id" in shot else None
        end_location_x = shot['end_location'][0] if "end_location" in shot else None
        end_location_y = shot['end_location'][1] if "end_location" in shot else None
        end_location_z = shot['end_location'][2] if "end_location" in shot and len(shot['end_location']) > 2 else None
        aerial_won = shot['aerial_won'] if "aerial_won" in shot else None
        follows_dribble = shot['follows_dribble'] if "follows_dribble" in shot else None
        first_time = shot['first_time'] if "first_time" in shot else None
        open_goal = shot['open_goal'] if "open_goal" in shot else None
        statsbomb_xg = shot['statsbomb_xg'] if "statsbomb_xg" in shot else None
        deflected = shot['deflected'] if "deflected" in shot else None
        technique = shot['technique']['id'] if "technique" in shot else None
        body_part = shot['body_part']['id'] if "body_part" in shot else None
        shot_type = shot['type']['id'] if "type" in shot else None
        outcome = shot['outcome']['id'] if "outcome" in shot else None
    else:
        key_pass_id = None
        end_location_x = None
        end_location_y = None
        end_location_z = None
        aerial_won = None
        follows_dribble = None
        first_time = None
        open_goal = None
        statsbomb_xg = None
        deflected = None
        technique = None
        body_part = None
        shot_type = None
        outcome = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO shot (eventid, keypassid, endlocationx, endlocationy, endlocationz, aerialwon, followsdribble, firsttime, opengoal, statsbombxg, deflected, technique, bodypart, shottype, outcome)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (event['id'], key_pass_id, end_location_x, end_location_y, end_location_z, aerial_won, follows_dribble, first_time, open_goal, statsbomb_xg, deflected, technique, body_part, shot_type, outcome))

def populate_pass(event):
    if 'pass' in event.keys():
        pass_event = event['pass']
        recipient = pass_event['recipient']['id'] if "recipient" in pass_event else None
        length = pass_event['length'] if "length" in pass_event else None
        angle = pass_event['angle'] if "angle" in pass_event else None
        height = pass_event['height']['id'] if "height" in pass_event else None
        end_location_x = pass_event['end_location'][0] if "end_location" in pass_event else None
        end_location_y = pass_event['end_location'][1] if "end_location" in pass_event else None
        end_location_z = pass_event['end_location'][2] if "end_location" in pass_event and len(pass_event['end_location']) > 2 else None
        assisted_shot = pass_event['assisted_shot_id'] if "assisted_shot_id" in pass_event else None
        backheel = pass_event['backheel'] if "backheel" in pass_event else None
        deflected = pass_event['deflected'] if "deflected" in pass_event else None
        miscommunication = pass_event['miscommunication'] if "miscommunication" in pass_event else None
        was_cross = pass_event['cross'] if "cross" in pass_event else None
        cut_back = pass_event['cut_back'] if "cut_back" in pass_event else None
        switch = pass_event['switch'] if "switch" in pass_event else None
        shot_assist = pass_event['shot_assist'] if "shot_assist" in pass_event else None
        goal_assist = pass_event['goal_assist'] if "goal_assist" in pass_event else None
        body_part = pass_event['body_part']['id'] if "body_part" in pass_event else None
        pass_type = pass_event['type']['id'] if "type" in pass_event else None
        outcome = pass_event['outcome']['id'] if "outcome" in pass_event else None
        technique = pass_event['technique']['id'] if "technique" in pass_event else None
    else:
        recipient = None
        length = None
        angle = None
        height = None
        end_location_x = None
        end_location_y = None
        end_location_z = None
        assisted_shot = None
        backheel = None
        deflected = None
        miscommunication = None
        was_cross = None
        cut_back = None
        switch = None
        shot_assist = None
        goal_assist = None
        body_part = None
        pass_type = None
        outcome = None
        technique = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO pass (eventid, recipient, length, angle, height, endlocationx, endlocationy, endlocationz, assisstedshot, backheel, deflected, miscommunication, wascross, cutback, switch, shotassist, goalassist, bodypart, passtype, outcome, technique)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (event['id'], recipient, length, angle, height, end_location_x, end_location_y, end_location_z, assisted_shot, backheel, deflected, miscommunication, was_cross, cut_back, switch, shot_assist, goal_assist, body_part, pass_type, outcome, technique))

def populate_player_off(event):
    if 'player_off' in event.keys():
        player_off = event['player_off']
        if "permanent" in player_off:
            permanent = player_off['permanent']
        else:
            permanent = None
    else:
        permanent = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO playeroff (eventid, permanent)
            VALUES (%s, %s)
        """, (event['id'], permanent))

def populate_pressure(event):
    if 'pressure' in event.keys():
        pressure = event['pressure']
        if "counterpress" in pressure:
            counterpress = pressure['counterpress']
        else:
            counterpress = None
    else:
        counterpress = None
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO pressure (eventid, counterpress)
            VALUES (%s, %s)
        """, (event['id'], counterpress))

def populate_shot_freeze_frame(event):
    if 'shot' in event.keys():
        if 'shot_freeze_frame' in event['shot'].keys():
            shot_freeze_frame = event['shot_freeze_frame']
            for frame in shot_freeze_frame:
                location_x = frame['location'][0] if "location" in frame else None
                location_y = frame['location'][1] if "location" in frame else None
                location_z = frame['location'][2] if "location" in frame and len(frame['location']) > 2 else None
                player = frame['player']['id'] if "player" in frame else None
                position_id = frame['position']['id'] if "position" in frame else None
                teammate = frame['teammate'] if "teammate" in frame else None
        else:
            location_x = None
            location_y = None
            location_z = None
            player = None
            position_id = None
            teammate = None
    else:
        location_x = None
        location_y = None
        location_z = None
        player = None
        position_id = None
        teammate = None

        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO shot_freeze_frame (eventid, locationx, locationy, locationz, player, positionid, teammate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (event['id'], location_x, location_y, location_z, player, position_id, teammate))


def populate_substitution(event):
    if 'substitution' in event.keys():
        substitution = event['substitution']
        replacement = substitution['replacement']['id'] if "replacement" in substitution else None
        outcome = substitution['outcome']['id'] if "outcome" in substitution else None
    else:
        replacement = None
        outcome = None

    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO substitution (eventid, replacement, outcome)
            VALUES (%s, %s, %s)
        """, (event['id'], replacement, outcome))
#Main
#Get Database Password
password = "1234"

#Establish Database Connection
try:
    conn = psycopg.connect(
        "dbname=ProjectDB user=postgres password=" + password + " host=localhost port=5432"
    )
except psycopg.OperationalError as e:
    print(f"Error: {e}")
    exit(1)

#Run Lookup Table Population Functions
populate_lookup()

#Run Main Table Population Functions
populate_competitions(data_filter)
populate_matches(data_filter_ids)
populate_lineups(data_filter_ids)
populate_events(data_filter_ids);
populate_360(data_filter_ids)

#Commit Changes
with conn.cursor() as cursor:
    conn.commit()