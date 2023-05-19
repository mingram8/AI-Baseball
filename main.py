
from pybaseball import statcast
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from pybaseball import playerid_reverse_lookup
from pybaseball import playerid_lookup
from pybaseball import schedule_and_record
import os.path
import pandas as pd
from pybaseball import get_splits
import subprocess

import sys

play_value = {}
play_value["strikeout"] = 0.0
play_value["walk"] = 0.0
play_value["hit_by_pitch"] = 0.0
play_value["field_out"] = 0.0
play_value["fielders_choice"] = 0.0
play_value["fielders_choice_out"] = 0.0
play_value["sac_fly"] = 0.0
play_value["force_out"] = 0.0
play_value["grounded_into_double_play"] = 0.0
play_value["strikeout_double_play"] = 0.0
play_value["sac_bunt"] = 0.0
play_value["wild_pitch"] = 0.0
play_value["single"] = 1.0
play_value["double"] = 2.0
play_value["triple"] = 3.0
play_value["home_run"] = 4.0
play_value["field_error"] = 4.0

# For weather API
city_name = {}
city_name["CIN"] = "Cincinnati"
city_name["CHC"] = "Chicago"
city_name["HOU"] = "Houston"
city_name["NYY"] = "New York"
city_name["LAD"] = "Los Angeles"
city_name["SD"] = "San Diego"
city_name["SEA"] = "Seattle"
city_name["OAK"] = "Oakland"
city_name["TOR"] = "Toronto"
city_name["TB"] = "Tampa Bay"
city_name["ATL"] = "Atlanta"
city_name["PHI"] = "Philadelphia"
city_name["LAA"] = "Los Angeles"
city_name["CWS"] = "Chicago"
city_name["AZ"] = "Phoenix"
city_name["CLE"] = "Cleveland"
city_name["MIN"] = "Minneapolis"
city_name["MIL"] = "Milwaukee"
city_name["COL"] = "Denver"
city_name["KC"] = "Kansas City"
city_name["TEX"] = "Dallas"
city_name["WSH"] = "Washington D.C."
city_name["PIT"] = "Pittsburgh"
city_name["NYM"] = "New York"
city_name["BOS"] = "Boston"
city_name["BAL"] = "Baltimore"
city_name["DET"] = "Detroit"
city_name["SF"] = "San Fransisco"
city_name["STL"] = "St. Louis"
city_name["MIA"] = "Miami"
city_name["MEX"] = "Mexico City"

# for other weather
city_latlon = {}
city_latlon["CIN"] = ["-84.5120", "39.1031"]
city_latlon["CHC"] = ["-87.6298", "41.8781"]
city_latlon["HOU"] = ["-95.3698", "29.7604"]
city_latlon["NYY"] = ["-74.0060", "40.7128"]
city_latlon["LAD"] = ["-118.2437", "34.0522"]
city_latlon["SD"] = ["-117.1611", "32.7157"]
city_latlon["SEA"] = ["-122.3321", "47.6062"]
city_latlon["OAK"] = ["-122.2712", "37.8044"]
city_latlon["TOR"] = ["-79.3832", "43.6532"]
city_latlon["TB"] = ["-82.3886", "28.2743"]
city_latlon["ATL"] = ["-84.3877", "33.7488"]
city_latlon["PHI"] = ["-75.1652", "39.9526"]
city_latlon["LAA"] = ["-118.2437", "34.0522"]
city_latlon["CWS"] = ["-87.6298", "41.8781"]
city_latlon["AZ"] = ["-112.0740", "33.4484"]
city_latlon["CLE"] = ["-81.6944", "41.4993"]
city_latlon["MIN"] = ["-93.2650", "44.9778"]
city_latlon["MIL"] = ["-87.9065", "43.0389"]
city_latlon["COL"] = ["-104.9903", "39.7392"]
city_latlon["KC"] = ["-94.5786", "39.0997"]
city_latlon["TEX"] = ["-97.1081", "32.7357"]
city_latlon["WSH"] = ["-77.0369", "38.9072"]
city_latlon["PIT"] = ["-79.9959", "40.4406"]
city_latlon["NYM"] = ["-74.0060", "40.7128"]
city_latlon["BOS"] = ["-71.0589", "42.3601"]
city_latlon["BAL"] = ["-76.6122", "39.2904"]
city_latlon["DET"] = ["-83.0458", "42.3314"]
city_latlon["SF"] = ["-122.4194", "37.7749"]
city_latlon["STL"] = ["-90.1994", "38.6270"]
city_latlon["MIA"] = ["-80.1918", "25.7617"]

park_name = {}
park_name["CIN"] = 1.0
park_name["CHC"] = 2.0
park_name["HOU"] = 3.0
park_name["NYY"] = 4.0
park_name["LAD"] = 5.0
park_name["SD"] = 6.0
park_name["SEA"] = 7.0
park_name["OAK"] = 8.0
park_name["TOR"] = 9.0
park_name["TB"] = 10.0
park_name["ATL"] = 11.0
park_name["PHI"] = 12.0
park_name["LAA"] = 13.0
park_name["CWS"] = 14.0
park_name["AZ"] = 15.0
park_name["CLE"] = 16.0
park_name["MIN"] = 17.0
park_name["MIL"] = 18.0
park_name["COL"] = 19.0
park_name["KC"] = 20.0
park_name["TEX"] = 21.0
park_name["WSH"] = 22.0
park_name["PIT"] = 23.0
park_name["NYM"] = 24.0
park_name["BOS"] = 25.0
park_name["BAL"] = 26.0
park_name["DET"] = 27.0
park_name["SF"] = 28.0
park_name["STL"] = 29.0
park_name["MIA"] = 30.0


# cache.enable()


def main_statcast(start, end):

    df = statcast(start_dt=start, end_dt=end, parallel=True)
    f = Path(start + '-' + end + '-statcast.csv')
    f.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(f)

def clean_up_statcast(filename):
    df = pd.read_csv(filename)

    new_df = pd.DataFrame()
    for index, row in df.iterrows():
        if not pd.isnull(row["events"]):
            data = playerid_reverse_lookup([row["batter"]], key_type='mlbam')
            batter_name = ""
            for i, r in data.iterrows():
                text = [r["name_first"], " ", r["name_last"]]
                batter_name = ''.join(text)
            try:
                new_df.at[index, "date"] = row["game_date"]
                new_df.at[index, "park"] = row["home_team"]
                new_df.at[index, "pitch_speed"] = row["release_speed"]
                new_df.at[index, "pitcher_name"] = row["player_name"]
                new_df.at[index, "pitcher_id"] = row["pitcher"]
                new_df.at[index, "batter_name"] = batter_name
                new_df.at[index, "batter_id"] = row["batter"]
                new_df.at[index, "event"] = row["events"]
                if pd.isnull(row["bb_type"]):
                    new_df.at[index, "type_of_hit"] = "none"
                new_df.at[index, "type_of_hit"] = row["bb_type"]
                new_df.at[index, "pitcher_hand"] = row["p_throws"]
                new_df.at[index, "batter_hand"] = row["stand"]
                new_df.at[index, "pitch_type"] = row["pitch_type"]
            except:
                continue
    new_df.to_csv("cleaned_up_" + filename, index=False)
    return "cleaned_up_" + filename


def get_season_stats(filename, batter, year):
    df = pd.read_csv(filename)
    batters = {}
    for index, row in df.iterrows():

        try:
            if batter == True:
                batters[row["batter_name"]] = {"id": row["batter_id"], "frame": pd.DataFrame()}
            else:
                batters[row["pitcher_name"]] = {"id": row["pitcher_id"], "frame": pd.DataFrame()}

        except:
            continue

    for k in batters:
        try:
            v = k.split()
            V = v[0].split(",")
            if batter == False:
                dd = playerid_lookup(V[0], v[1])
            else:
                dd = playerid_lookup(v[1], v[0])
        except:
            continue

        for i, r in dd.iterrows():

            if r['key_mlbam'] == batters[k]["id"]:
                if batter == False:
                    batters[k]["frame"] = get_splits(r['key_bbref'], year, pitching_splits=True)[0]
                else:
                    batters[k]["frame"] = get_splits(r['key_bbref'], year)
                if batter == False:
                    dff = batters[k]["frame"]
                    df2 = pd.DataFrame(dff)
                    df2.to_csv(k + "-pitcher-2022.csv")
                else:
                    batters[k]["frame"].to_csv(k + "-2022.csv")



def add_other_weather(filename, year):
    df = pd.read_csv(filename)
    weather = []
    oldDate = ""
    dates = []
    weather_map = {}
    for index, row in df.iterrows():
        if oldDate != row["date"] + row["park"]:
            oldDate = row["date"] + row["park"]
            park = city_latlon[row["park"]]
            date = row["date"]
            done = False
            for v in weather_map:
                if v == row["park"]:
                    done = True
            if done == False:
                url = "https://archive-api.open-meteo.com/v1/era5?latitude=" + park[1] + "&longitude=" + park[
                    0] + "&start_date=" + str(year) + "-03-29" + "&end_date=" + str(
                    year) + "-09-30" + "&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&temperature_unit=fahrenheit"
                response = requests.get(url)
                response_json = response.json()
                map = parse_api(response_json, park)
                weather_map[row["park"]] = map

    for index, row in df.iterrows():
        date = row["date"]
        day_game_temp = weather_map[row["park"]][date]["day_game_temp"]
        day_game_wind = weather_map[row["park"]][date]["day_game_wind"]
        day_game_humidity = weather_map[row["park"]][date]["day_game_humidity"]
        night_game_temp = weather_map[row["park"]][date]["night_game_temp"]
        night_game_wind = weather_map[row["park"]][date]["night_game_wind"]
        night_game_humidity = weather_map[row["park"]][date]["night_game_humidity"]
        weather.append([date, row["park"], day_game_temp, day_game_wind, day_game_humidity,
                        night_game_temp, night_game_wind, night_game_humidity])
        new_pd = pd.DataFrame(weather, columns=['date', 'park', 'day_game_temp', 'day_game_wind', 'day_game_humidity',
                                                'night_game_temp', 'night_game_wind', 'night_game_humidity'])
    new_pd.to_csv("weather-" + filename, index=False)


def parse_api(resp, park):
    giant_date_map = {}
    hours = resp["hourly"]["time"]
    temps = resp["hourly"]["temperature_2m"]
    wind = resp["hourly"]["windspeed_10m"]
    humidity = resp["hourly"]["relativehumidity_2m"]
    day_game_temp = []
    day_game_wind = []
    day_game_humidity = []
    night_game_temp = []
    night_game_wind = []
    night_game_humidity = []
    for index, t in enumerate(hours):
        date = t.split("T")
        write = False
        if date[1] == "13:00":
            day_game_temp = temps[index]
            day_game_wind = wind[index]
            day_game_humidity = humidity[index]
            write = True
        if date[1] == "21:00":
            night_game_temp = temps[index]
            night_game_wind = wind[index]
            night_game_humidity = humidity[index]
            write = True
        if write == True:
            giant_date_map[date[0]] = {"day_game_temp": day_game_temp, "day_game_wind": day_game_wind,
                                       "day_game_humidity": day_game_humidity, "night_game_temp": night_game_temp,
                                       "night_game_wind": night_game_wind, "night_game_humidity": night_game_humidity}
    return giant_date_map


def add_weather(filename):
    key = "a528583423d04e7aa71192549231204"
    df = pd.read_csv(filename)
    weather = []
    oldDate = ""
    for index, row in df.iterrows():
        if oldDate != row["date"] + row["park"]:
            oldDate = row["date"] + row["park"]
            park = city_name[row["park"]]
            date = row["date"]
            url = "https://api.weatherapi.com/v1/history.json?key=" + key + "&q=" + park + "&dt=" + date
            response = requests.get(url)

            response_json = response.json()
            try:
                day_game = response_json["forecast"]["forecastday"][0]["hour"][13]
                night_game = response_json["forecast"]["forecastday"][0]["hour"][19]
                weather.append([date, row["park"], day_game["temp_f"], day_game["wind_mph"], day_game["humidity"],
                                night_game["temp_f"], night_game["wind_mph"], night_game["humidity"]])
            except:
                continue
        else:
            continue
    new_pd = pd.DataFrame(weather, columns=['date', 'park', 'day_game_temp', 'day_game_wind', 'day_game_humidity',
                                            'night_game_temp', 'night_game_wind', 'night_game_humidity'])
    new_pd.to_csv("weather-" + filename, index=False)


def get_day_night(year):
    dates = []
    for k in city_name:
        newK = k
        if k == "CWS":
            newK = "CHW"
        if k == "AZ":
            # new_pd = pd.DataFrame(dates)
            # new_pd.to_csv("DATES-2022.csv")
            newK = "ARI"
        if k == "WSH":
            newK = "WSN"

        try:
            data = schedule_and_record(year, newK)
            for index, row in data.iterrows():
                row['Date'] = row['Date'].split("(")[0]
                # row['Date'] = pd.to_datetime(row['Date'], format='%A, %b %d')
                # row['Date'] = row['Date'].strftime('%m-%d')
                r = [str(year) + " " + row['Date'], k, row["D/N"]]
                dates.append(r)
                new_pd = pd.DataFrame(dates, columns=['Date', 'Team', 'D/N'])
                new_pd.to_csv("dates-" + str(year) + ".csv", index=False)
        except:
            continue


def fix_dates(year):
    df = pd.read_csv("dates-" + str(year) + ".csv")
    for index, row in df.iterrows():
        date = row["Date"].split("-")[0]
        date = fix_date(date)
        df.at[index, "Date"] = date

    df.to_csv("dates-" + str(year) + ".csv", index=False)


def pad_zero(day):
    d = int(day)
    if d < 10:
        day = "0" + day
    return day


def fix_date(date):
    sep = date.split(",")
    month = sep[1].split()[0]
    newMonth = checkMonth(month)

    year = sep[0].split()[0]
    day = sep[1].split()[1]

    return str(year) + "-" + newMonth + "-" + pad_zero(day)


def checkMonth(month):
    if month == "Jan":
        return "01"
    elif month == "Feb":
        return "02"
    elif month == "Mar":
        return "03"
    elif month == "Apr":
        return "04"
    elif month == "May":
        return "05"
    elif month == "Jun":
        return "06"
    elif month == "Jul":
        return "07"
    elif month == "Aug":
        return "08"
    elif month == "Sep":
        return "09"
    elif month == "Oct":
        return "10"


def line_up_times(filename, year):
    df = pd.read_csv(filename)
    df2 = pd.read_csv("dates-" + (year) + ".csv")
    for index, row in df.iterrows():
        date = row["date"]
        park = row["park"]
        night = df2.loc[(df2['Date'] == date) & (df2['Team'] == park)]
        try:
            df.at[index, "D/N"] = night['D/N'].values[0]
        except:
            df.at[index, "D/N"] = "N"

    df.to_csv(filename, index=False)


# fix_dates()
def get_batter_splits_all(filename, year):
    df = pd.read_csv(filename)
    pitchers = {}
    for index, row in df.iterrows():
        pitchers[row["batter_name"]] = {"id": row["batter_id"], "frame": pd.DataFrame()}
    for k in pitchers:
        try:
            dd = playerid_reverse_lookup([pitchers[k]["id"]])
        except:
            continue


        for i, r in dd.iterrows():
            if os.path.isfile(k + "-" + r['key_bbref'] + "-" + str(year) + ".csv") == True:
                print("already is a file")
                continue
            try:
                frame = get_splits(r['key_bbref'], str(year), pitching_splits=False)
                pitchers[k]["frame"] = frame
                dff = pitchers[k]["frame"]
                df2 = pd.DataFrame(dff)
                df2.to_csv(k.lower() + "-" + r['key_bbref'] + "-" + str(year) + ".csv")
            except Exception as e:
                print(e)



def get_batter_splits(name, id, year):
    dd2 = playerid_reverse_lookup([id], "fangraphs")

    for index, row in dd2.iterrows():
        df = get_splits(row['key_bbref'], year, pitching_splits=False)
        df2 = pd.DataFrame(df)

        df2.to_csv(name.lower() + "-" + row['key_bbref'] + "-" + str(year) + ".csv")


def get_pitcher_splits(name, id, year):
    dd2 = playerid_reverse_lookup([id], "fangraphs")

    for index, row in dd2.iterrows():
        df = get_splits(row['key_bbref'], year, pitching_splits=True)[0]
        df2 = pd.DataFrame(df)

        df2.to_csv(name.lower() + "-pitcher-" + row['key_bbref'] + "-" + str(year) + ".csv")


def get_pitcher_splits_all(filename, year):
    df = pd.read_csv(filename)
    pitchers = {}
    for index, row in df.iterrows():
        pitchers[row["pitcher_name"]] = {"id": row["pitcher_id"], "frame": pd.DataFrame()}
    for k in pitchers:
        try:
            dd = playerid_reverse_lookup([pitchers[k]["id"]])
        except:
            continue

        for i, r in dd.iterrows():
            if os.path.isfile(k.lower() + "-pitcher-" + r['key_bbref'] + "-" + str(year) + ".csv") == True:
                continue
            try:
                pitchers[k]["frame"] = get_splits(r['key_bbref'], year, pitching_splits=True)[0]
                dff = pitchers[k]["frame"]
                df2 = pd.DataFrame(dff)
                df2.to_csv(k.lower() + "-pitcher-" + r['key_bbref'] + "-" + str(year) + ".csv")
            except Exception as e:
                print(e)


def get_pitcher_splits(name, id, year):
    dd2 = playerid_reverse_lookup([id], "fangraphs")
    print(id)
    for index, row in dd2.iterrows():
        df = get_splits(row['key_bbref'], year, pitching_splits=True)[0]
        df2 = pd.DataFrame(df)

        df2.to_csv(name.lower() + "-pitcher-" + row['key_bbref'] + "-" + str(year) + ".csv")


def add_weather_game(filename):
    df = pd.read_csv(filename)
    file = "weather-" + filename
    df2 = pd.read_csv(file)
    for index, row in df.iterrows():
        print(row["date"], row["park"])
        night = df2.loc[(df2['date'] == row["date"]) & (df2['park'] == row["park"])]
        if row["D/N"] == "N":
            temp = night["night_game_temp"].values[0]
            wind = night["night_game_wind"].values[0]
            humidity = night["night_game_humidity"].values[0]
        else:
            temp = night["day_game_temp"].values[0]
            wind = night["day_game_wind"].values[0]
            humidity = night["day_game_humidity"].values[0]
        print(temp)
        df.at[index, "temp"] = temp
        df.at[index, "wind"] = wind
        df.at[index, "humidity"] = humidity

    df.to_csv(filename, index=False)


def fix_csv(file):
    df = pd.read_csv(file)
    df_filtered = df[pd.isnull(df['ba']) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["pba"]) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["phr"]) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["hr"]) == False]

    df_filtered.to_csv(file, index=False)


def make_testdata(filename, homer=True, batch=True):
    i = int(0)
    df = pd.read_csv(filename)
    df = df[pd.isnull(df['BA']) == False]
    df = df[pd.isnull(df["pBA"]) == False]
    df = df[pd.isnull(df["pHR"]) == False]
    df = df[pd.isnull(df["HR"]) == False]
    df = df[pd.isnull(df["pPA"]) == False]
    df = df[pd.isnull(df["PA"]) == False]

    # The csv comes in last to first, we need to flip it
    df.reindex(index=df.index[::-1])
    testdata = pd.DataFrame()
    game = {"date": "0/0/0"}
    for index, row in df.iterrows():
        if row["date"] != game["date"]:
            if game["date"] != "0/0/0" and batch == True:
                for k in game:
                    if k == "date":
                        continue
                    inn = i
                    testdata.at[inn, "batter_hand"] = game[k]["batter_hand"]
                    testdata.at[inn, "pitcher_hand"] = game[k]["pitcher_hand"]
                    testdata.at[inn, "temp"] = game[k]["temp"]
                    testdata.at[inn, "humidity"] = game[k]["humidity"]
                    testdata.at[inn, "wind"] = game[k]["wind"]
                    testdata.at[inn, "ba"] = game[k]["ba"]
                    testdata.at[inn, "slg"] = game[k]["slg"]
                    testdata.at[inn, "hr"] = game[k]["hr"]
                    testdata.at[inn, "hit"] = game[k]["hit"]
                    testdata.at[inn, "double"] = game[k]["double"]
                    testdata.at[inn, "triple"] = game[k]["triple"]

                    testdata.at[inn, "pba"] = game[k]["pba"]
                    testdata.at[inn, "pslg"] = game[k]["pslg"]
                    testdata.at[inn, "phr"] = game[k]["phr"]
                    testdata.at[inn, "pPA"] = game[k]["pPA"]
                    testdata.at[inn, "pa"] = game[k]["pa"]

                    testdata.at[inn, "park"] = game[k]["park"]
                    testdata.at[inn, "output"] = game[k]["output"]
                    testdata.at[inn, "d_rate"] = game[k]["d_rate"]
                    testdata.at[inn, "t_rate"] = game[k]["t_rate"]
                    testdata.at[inn, "pd_rate"] = game[k]["pDouble"]
                    testdata.at[inn, "pt_rate"] = game[k]["pTriple"]
                    testdata.at[inn, "PA_game"] = game[k]["PA_game"]

                    i += 1
            game = {"date": row["date"]}

        try:
            p = play_value[row["event"]]
        except:
            continue
        play_val = 0.0
        # testdata["park"] = park_name[row["park"]]
        if row["batter_hand"] == "L":
            batter_hand = 1.0
        else:
            batter_hand = 0.0
        if row["pitcher_hand"] == "R":
            pitcher_hand = 1.0
        else:
            pitcher_hand = 0.0
        if batch == False:
            testdata.at[index, "batter_hand"] = batter_hand
            testdata.at[index, "pitcher_hand"] = pitcher_hand
            testdata.at[index, "temp"] = row["temp"]
            testdata.at[index, "humidity"] = row["humidity"]
            testdata.at[index, "wind"] = row["wind"]
            testdata.at[index, "ba"] = row["BA"]
            testdata.at[index, "slg"] = row["SLG"]
            testdata.at[index, "hr"] = row["HR"]
            testdata.at[index, "hr"] = row["HR"]
            testdata.at[index, "pa"] = row["PA"]
            testdata.at[index, "pba"] = row['pBA']
            testdata.at[index, "pslg"] = row['pSLG']
            testdata.at[index, "phr"] = row["pHR"]
            testdata.at[index, "pPA"] = row["pPA"]
            # Only care about homers or all hits
            if homer == True:
                if play_value[row['event']] == 4.0:
                    play_val = 1.0
                else:
                    play_val = 0.0
            else:
                play_val = play_value[row['event']]
            testdata.at[index, "output"] = play_val
        else:
            hit = 0.0
            double = 0.0
            triple = 0.0
            if homer == True:
                if play_value[row['event']] == 4.0:
                    play_val = 1.0
                else:
                    play_val = 0.0
                if play_value[row['event']] == 1.0:
                    hit = 1.0
                if play_value[row['event']] == 2.0:
                    double = 2.0
                if play_value[row['event']] == 3.0:
                    triple = 3.0
            else:
                play_val = play_value[row['event']]
            try:
                # dont change everything for batch since most PA are starters
                v = game[row["batter_id"]]["output"]
                vv = game[row["batter_id"]]["PA_game"]
                vv += 1
                v += play_val

                h = game[row["batter_id"]]["hit"]
                d = game[row["batter_id"]]["double"]
                t = game[row["batter_id"]]["triple"]
                h += hit
                d += double
                t += triple
                game[row["batter_id"]]["PA_game"] = vv
                game[row["batter_id"]]["output"] = v
                game[row["batter_id"]]["hit"] = h
                game[row["batter_id"]]["double"] = d
                game[row["batter_id"]]["triple"] = t
                game[row["batter_id"]]["pPA"] += row["pPA"]
                game[row["batter_id"]]["phr"] += row["pHR"]
                game[row["batter_id"]]["pslg"] += row["pSLG"]
                game[row["batter_id"]]["pba"] += row["pBA"]
                game[row["batter_id"]]["pDouble"] += row["pDouble"]
                game[row["batter_id"]]["pTriple"] += row["pTriple"]

            except:
                hr = row["HR"]
                phr = row["pHR"]
                game[row["batter_id"]] = {"PA_game": 1, "ba": row["BA"], "slg": row["SLG"], "hr": hr, "pa": row['PA'],
                                          "d_rate": row["double"], "t_rate": row["triple"], "hit": hit,
                                          "double": double, "triple": triple,
                                          "pPA": row['pPA'], "pDouble": row['pDouble'], "pTriple": row['pTriple'],
                                          "park": park_name[row["park"]],
                                          "phr": phr, "pba": row['pBA'], "pslg": row["pSLG"],
                                          "temp": row["temp"], "wind": row["wind"], "humidity": row["humidity"],
                                          "batter_hand": batter_hand, "pitcher_hand": pitcher_hand, "output": play_val}

    testdata.to_csv("testdata-" + filename, index=False)


def make_big_testdata(filename, homer=True, batch=True):
    i = int(0)
    game = {"date": "0/0/0"}
    testdata = pd.DataFrame()
    for f in filename:
        print(f)
        df = pd.read_csv(f)
        df = df[pd.isnull(df['BA']) == False]
        df = df[pd.isnull(df["pBA"]) == False]
        df = df[pd.isnull(df["pHR"]) == False]
        df = df[pd.isnull(df["HR"]) == False]
        for index, row in df.iterrows():
            if row["date"] != game["date"]:
                if game["date"] != "0/0/0" and batch == True:
                    for k in game:
                        if k == "date":
                            continue
                        # PA index is different than every pitch index
                        inn = i
                        testdata.at[inn, "batter_hand"] = game[k]["batter_hand"]
                        testdata.at[inn, "pitcher_hand"] = game[k]["pitcher_hand"]
                        testdata.at[inn, "temp"] = game[k]["temp"]
                        testdata.at[inn, "humidity"] = game[k]["humidity"]
                        testdata.at[inn, "wind"] = game[k]["wind"]
                        testdata.at[inn, "ba"] = game[k]["ba"]
                        testdata.at[inn, "slg"] = game[k]["slg"]
                        testdata.at[inn, "hr"] = game[k]["hr"]
                        testdata.at[inn, "pba"] = game[k]["pba"]
                        testdata.at[inn, "pslg"] = game[k]["pslg"]
                        testdata.at[inn, "phr"] = game[k]["phr"]
                        testdata.at[inn, "output"] = game[k]["output"]

                        i += 1
                game = {"date": row["date"]}

            try:
                p = play_value[row["event"]]
            except:
                # Things that don't end the PA are events like runner thrown out to end inning, we ignore them
                continue
            play_val = 0.0
            testdata["park"] = park_name[row["park"]]
            if row["batter_hand"] == "L":
                batter_hand = 1.0
            else:
                batter_hand = 0.0
            if row["pitcher_hand"] == "R":
                pitcher_hand = 1.0
            else:
                pitcher_hand = 0.0
            if batch == False:
                testdata.at[index, "batter_hand"] = batter_hand
                testdata.at[index, "pitcher_hand"] = pitcher_hand
                testdata.at[index, "temp"] = row["temp"]
                testdata.at[index, "humidity"] = row["humidity"]
                testdata.at[index, "wind"] = row["wind"]
                testdata.at[index, "ba"] = row["BA"]
                testdata.at[index, "slg"] = row["SLG"]
                testdata.at[index, "hr"] = row["HR"] / row['PA']
                testdata.at[index, "pba"] = row['pBA']
                testdata.at[index, "pslg"] = row['pSLG']
                testdata.at[index, "phr"] = row["pHR"] / row['pPA']
                if homer == True:
                    if play_value[row['event']] == 4.0:
                        play_val = 1.0
                    else:
                        play_val = 0.0
                else:
                    play_val = play_value[row['event']]
                testdata.at[index, "output"] = play_val
            else:
                if homer == True:
                    if play_value[row['event']] == 4.0:
                        play_val = 1.0
                    else:
                        play_val = 0.0
                else:
                    play_val = play_value[row['event']]
                try:
                    v = game[row["batter_id"]]["output"]
                    if v == 0.0:
                        game[row["batter_id"]]["output"] = play_val
                except:
                    hr = row["HR"] / row['PA']
                    phr = row["pHR"] / row['pPA']

                    game[row["batter_id"]] = {"ba": row["BA"], "slg": row["SLG"], "hr": hr,
                                              "phr": phr, "pba": row['pBA'], "pslg": row["pSLG"],
                                              "temp": row["temp"], "wind": row["wind"], "humidity": row["humidity"],
                                              "batter_hand": batter_hand, "pitcher_hand": pitcher_hand,
                                              "output": play_val}

    testdata.to_csv("testdata-total.csv", index=False)


def add_pitcher_stats(filename, year):
    df = pd.read_csv(filename)
    for index, row in df.iterrows():
        player = row["pitcher_name"].lower()
        dd = playerid_reverse_lookup([row["pitcher_id"]])
        for i, r in dd.iterrows():
            id = r["key_bbref"]
            file = player + "-pitcher-" + id + "-" + str(year) + ".csv"
            if os.path.isfile(file) == False:
                continue

            df2 = pd.read_csv(file)
            if row["batter_hand"] == "R":
                r = df2.loc[(df2['Split'] == "vs RHB")]
                hr = r["HR"]
                ba = r["BA"]
                slg = r["SLG"]
                pa = r["PA"]
                double = r["2B"]
                triple = r["3B"]
            else:
                r = df2.loc[(df2['Split'] == "vs LHB")]
                hr = r["HR"]
                ba = r["BA"]
                slg = r["SLG"]
                pa = r["PA"]
                double = r["2B"]
                triple = r["3B"]
            try:
                df.at[index, "pBA"] = ba
                df.at[index, "pSLG"] = slg
                df.at[index, "pHR"] = hr
                df.at[index, "pPA"] = pa
                df.at[index, "pDouble"] = double
                df.at[index, "pTriple"] = triple
            except:
                continue

    df.to_csv(filename, index=False)


def add_batter_stats(filename, year):
    df = pd.read_csv(filename)
    ba = 0.0
    slg = 0.0
    hr = 0.0
    pa = 0.0

    for index, row in df.iterrows():
        player = row["batter_name"]
        dd = playerid_reverse_lookup([row["batter_id"]])
        for i, r in dd.iterrows():
            id = r["key_bbref"]
            file = player + "-" + id + "-" + str(year) + ".csv"
            try:
                df2 = pd.read_csv(file)
            except:
                print("no file")
                continue
            if row["pitcher_hand"] == "R":
                try:
                    r = df2.loc[(df2['Split'] == "vs RHP")]
                    hr = r["HR"]
                    ba = r["BA"]
                    slg = r["SLG"]
                    pa = r["PA"]
                    double = r["2B"]
                    triple = r["3B"]
                except:
                    continue
            else:
                try:
                    r = df2.loc[(df2['Split'] == "vs LHP")]
                    hr = r["HR"]
                    ba = r["BA"]
                    slg = r["SLG"]
                    pa = r["PA"]
                    double = r["2B"]
                    triple = r["3B"]
                except:
                    continue
        try:
            df.at[index, "BA"] = ba
            df.at[index, "SLG"] = slg
            df.at[index, "HR"] = hr
            df.at[index, "PA"] = pa
            df.at[index, "double"] = double
            df.at[index, "triple"] = triple

        except:
            continue

    df.to_csv(filename, index=False)


def get_id(link):
    id = link.split("playerid=")
    id = id[1].split("&")
    return id[0]


def get_line_up(date, game, park):
    url = "http://www.fangraphs.com/livescoreboard.aspx?date=" + date
    print(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    results = soup.find_all("table", {"class": "lineup"})
    game_num = 0
    for result in results:
        res = result.find_all("td", {"align": "left"})
        index = 0
        lineup = {}
        home_pitcher = ""
        away_pitcher = ""
        for r in res:
            for link in r.find_all('a', href=True):
                if index == 0:
                    away_pitcher = get_id(link['href'])
                if index == 1:
                    lineup["pitcher"] = get_id(link['href'])
                if index == 2:
                    lineup["one"] = get_id(link['href'])
                if index == 3:
                    lineup["two"] = get_id(link['href'])
                if index == 4:
                    lineup["three"] = get_id(link['href'])
                if index == 5:
                    lineup["four"] = get_id(link['href'])
                if index == 6:
                    lineup["five"] = get_id(link['href'])
                if index == 7:
                    lineup["six"] = get_id(link['href'])
                if index == 8:
                    lineup["seven"] = get_id(link['href'])
                if index == 9:
                    lineup["eight"] = get_id(link['href'])
                if index == 10:
                    lineup["nine"] = get_id(link['href'])
                if index == 11:
                    lineup["h_pitcher"] = away_pitcher
                    lineup["a_one"] = get_id(link['href'])
                if index == 12:
                    lineup["a_two"] = get_id(link['href'])
                if index == 13:
                    lineup["a_three"] = get_id(link['href'])
                if index == 14:
                    lineup["a_four"] = get_id(link['href'])
                if index == 15:
                    lineup["a_five"] = get_id(link['href'])
                if index == 16:
                    lineup["a_six"] = get_id(link['href'])
                if index == 17:
                    lineup["a_seven"] = get_id(link['href'])
                if index == 18:
                    lineup["a_eight"] = get_id(link['href'])
                if index == 19:
                    lineup["a_nine"] = get_id(link['href'])
                index += 1

        if game_num == game:
            lineup["park"] = park
            df = pd.DataFrame([lineup])
            df.to_json("lineup.json", orient='records')
        game_num += 1


def get_today_weather(location, date, day=False):
    key = "a528583423d04e7aa71192549231204"
    weather = []
    url = "https://api.weatherapi.com/v1/forecast.json?key=" + key + "&q=" + location
    response = requests.get(url)

    response_json = response.json()
    try:
        day_game = response_json["forecast"]["forecastday"][0]["hour"][13]
        if day == True:
            return day_game["temp_f"], day_game["humidity"], day_game["wind_mph"]
        else:
            night_game = response_json["forecast"]["forecastday"][0]["hour"][19]
            return night_game["temp_f"], night_game["humidity"], night_game["wind_mph"]
    except:
        return {}, {}


def line_up_json(file, day=False, year="2022", force=False, park="TEX"):
    df = pd.read_json(file)
    print(df.columns)
    lineup_data = pd.DataFrame(
        columns=["temp", "wind", "humidity", "ba", "slg", "hr", "pa", "pba", "pslg", "phr", "pPA", "batter_hand",
                 "pitcher_hand", "park", "name", "double", "triple", "pDouble", "pTriple"])
    i = 0
    for index, r in df.iterrows():
        away_pitcher, name = get_player_id(r["pitcher"], True, year, force)
        park = r["park"]
        if away_pitcher.empty == False:
            h1, name = get_player_id(r["one"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h1, i, park, name, day)
            i += 1
            h2, name = get_player_id(r["two"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h2, i, park, name, day)
            i += 1
            h3, name = get_player_id(r["three"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h3, i, park, name, day)
            i += 1
            h4, name = get_player_id(r["four"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h4, i, park, name, day)
            i += 1
            h5, name = get_player_id(r["five"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h5, i, park, name, day)
            i += 1
            h6, name = get_player_id(r["six"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h6, i, park, name, day)
            i += 1
            h7, name = get_player_id(r["seven"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h7, i, park, name, day)
            i += 1
            h8, name = get_player_id(r["eight"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h8, i, park, name, day)
            i += 1
            h9, name = get_player_id(r["nine"], False, year, force)
            lineup_data = make_row(lineup_data, away_pitcher, h9, i, park, name, day)
            i += 1
        #really is the away pitcher but for home batters
        h_pitcher, name = get_player_id(r["h_pitcher"], True, year, force)

        if h_pitcher.empty == False:
            a1, name = get_player_id(r["a_one"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a1, i, park, name, day)
            i += 1
            a2, name = get_player_id(r["a_two"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a2, i, park, name, day)
            i += 1
            a3, name = get_player_id(r["a_three"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a3, i, park, name, day)
            i += 1
            a4, name = get_player_id(r["a_four"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a4, i, park, name, day)
            i += 1
            a5, name = get_player_id(r["a_five"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a5, i, park, name, day)
            i += 1
            a6, name = get_player_id(r["a_six"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a6, i, park, name, day)
            i += 1
            a7, name = get_player_id(r["a_seven"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a7, i, park, name, day)
            i += 1
            a8, name = get_player_id(r["a_eight"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a8, i, park, name, day)
            i += 1
            a9, name = get_player_id(r["a_nine"], False, year, force)
            lineup_data = make_row(lineup_data, h_pitcher, a9, i, park, name, day)

            i += 1

    lineup_data.to_csv("lineup.csv", index=False)


def make_row(lineup_data, away_pitcher, h1, index, park, name, day):
    stats = pd.DataFrame()
    p_stats = pd.DataFrame()
    try:
        if h1.empty == True:
            return lineup_data
        s_pitcher_hand = True
        hand = away_pitcher.loc[(away_pitcher['Split'] == "vs RHB as LHP")]
        if hand.empty == False:
            s_pitcher_hand = False
        hand = away_pitcher.loc[(away_pitcher['Split'] == "vs LHB as LHP")]
        if hand.empty == False:
            s_pitcher_hand = False
        hitter_hand = True
        if s_pitcher_hand == True:
            stats = h1.loc[(h1['Split'] == "vs RHP")]
            hand = h1.loc[(h1['Split'] == "vs LHP as LHB")]
            if hand.empty == False:
                hitter_hand = False
            hand = h1.loc[(h1['Split'] == "vs RHP as LHB")]
            if hand.empty == False:
                hitter_hand = False
        else:
            stats = h1.loc[(h1['Split'] == "vs LHP")]
            hand = h1.loc[(h1['Split'] == "vs LHP as LHB")]
            if hand.empty == False:
                hitter_hand = False
            hand = h1.loc[(h1['Split'] == "vs RHP as LHB")]
            if hand.empty == False:
                hitter_hand = False
        if hitter_hand == True:
            p_stats = away_pitcher.loc[(away_pitcher['Split'] == "vs RHB")]
        else:
            p_stats = away_pitcher.loc[(away_pitcher['Split'] == "vs LHB")]
        temp, humidity, wind = get_today_weather(city_name[park], "2022-04-19", day)
        lineup_data.at[index, "batter_hand"] = 1.0
        lineup_data.at[index, "pitcher_hand"] = 0.0
        lineup_data.at[index, "temp"] = temp
        lineup_data.at[index, "humidity"] = humidity
        lineup_data.at[index, "wind"] = wind


        lineup_data.at[index, "ba"] = stats["BA"].values[0]
        lineup_data.at[index, "slg"] = stats["SLG"].values[0]
        lineup_data.at[index, "hr"] = stats["HR"].values[0]
        lineup_data.at[index, "pba"] = p_stats["BA"].values[0]
        lineup_data.at[index, "pslg"] = p_stats["SLG"].values[0]
        lineup_data.at[index, "pPA"] = p_stats["PA"].values[0]
        lineup_data.at[index, "phr"] = p_stats["HR"].values[0]
        lineup_data.at[index, "pa"] = stats["PA"].values[0]
        lineup_data.at[index, "park"] = park
        lineup_data.at[index, "name"] = name
        lineup_data.at[index, "double"] = stats["2B"].values[0]
        lineup_data.at[index, "triple"] = stats["3B"].values[0]
        lineup_data.at[index, "pDouble"] = p_stats["2B"].values[0]
        lineup_data.at[index, "pTriple"] = p_stats["3B"].values[0]

    except:
        temp, humidity, wind = get_today_weather(city_name[park], "2022-04-17")

        lineup_data.at[index, "batter_hand"] = 1.0
        lineup_data.at[index, "pitcher_hand"] = 0.0
        lineup_data.at[index, "temp"] = temp
        lineup_data.at[index, "humidity"] = humidity
        lineup_data.at[index, "wind"] = wind
        lineup_data.at[index, "ba"] = 0.0
        lineup_data.at[index, "slg"] = 0.0
        lineup_data.at[index, "hr"] = 0.0
        lineup_data.at[index, "pba"] = 0.0
        lineup_data.at[index, "pslg"] = 0.0
        lineup_data.at[index, "pPA"] = 1.0
        lineup_data.at[index, "phr"] = 0.0
        lineup_data.at[index, "pa"] = 1.0
        lineup_data.at[index, "park"] = park
        lineup_data.at[index, "name"] = name
        lineup_data.at[index, "double"] = 0.0
        lineup_data.at[index, "triple"] = 0.0
        lineup_data.at[index, "pDouble"] = 0.0
        lineup_data.at[index, "pTriple"] = 0.0

    return lineup_data


def get_player_id(id, pitcher=False, year="2022", force=False):
    p = playerid_reverse_lookup([id], "fangraphs")
    try:
        for i, row in p.iterrows():
            id = row["key_bbref"]
            if pitcher == True:
                file = row["name_last"] + ", " + row["name_first"] + "-pitcher-" + id + "-" + year + ".csv"
            else:
                file = row["name_first"] + " " + row["name_last"] +"-"+id + "-" + year + ".csv"

            return get_player(file, row["key_fangraphs"], row["name_first"] + " " + row["name_last"],pitcher, force, int(year)), row["name_first"] + " " + row["name_last"]
        return pd.DataFrame(), "sucks"
    except:
        return pd.DataFrame(), "sucks"

def get_player(file, id, name, pitcher=False, force=False, year=2022):
    df2 = pd.DataFrame()
    try:
        if os.path.isfile(file) == True:
            if force== False:
                df2 = pd.read_csv(file)
            else:
                if pitcher == False:
                    get_batter_splits(name, id, year)
                    f = file.split("2022")
                    f = f[0] + "2023"
                    df2 = pd.read_csv(file)
                else:
                    n = name.split(" ")
                    name = n[1] + ", " + n[0]
                    get_pitcher_splits(name, id, year)
                    df2 = pd.read_csv(file)
        else:
            if pitcher == False:
                get_batter_splits(name, id, year)
                f = file.split("2022")
                f = f[0] + "2023"
                df2 = pd.read_csv(file)
            else:
                n = name.split(" ")
                name = n[1]+", "+n[0]
                get_pitcher_splits(name, id, year)
                df2 = pd.read_csv(file)
        return df2
    except:
        return df2


years = [["2005-03-29", "2005-09-30"]]

do = sys.argv[1]
team = sys.argv[2]
day = sys.argv[3]
index = sys.argv[4]
date = sys.argv[5]
if do == "t":
    get_line_up(date, int(index), team)

if day=="t":
    line_up_json("./lineup.json", True, "2022", False)
else:
    line_up_json("./lineup.json", False, "2022", False)
p = subprocess.Popen(['/usr/local/bin/node', '../brain-baseball/index.js', '--team='+team], stdout=subprocess.PIPE)
out = p.stdout.read()
print(out)
if day=="t":
    line_up_json("./lineup.json", True, "2023", True)
else:
    line_up_json("./lineup.json", False, "2023", True)

p = subprocess.Popen(['/usr/local/bin/node', '../brain-baseball/index.js', '--team='+team], stdout=subprocess.PIPE)
out = p.stdout.read()
print(out)
