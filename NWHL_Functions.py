# -*- coding: utf-8 -*-
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#     NWHL Functions
#
#     Used for data from the NWHL
#     
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

import os
import sys
import warnings

import numpy as np
import pandas as pd
import requests
from lxml import html
from pandas import DataFrame, Series, Timestamp

sys.path.append('/Users/Mike/Desktop/Personal/other/refactorpy.github.io/NWHL')

warnings.filterwarnings('ignore')

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#
#                     GLOBAL VARIABLES
#
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = CURRENT_DIR + '/Team_Data'

# https://pride.nwhl.zone/stats#/100/team/61638/stats
BOS = pd.read_csv(DATA_DIR + '/bos.csv')
# https://beauts.nwhl.zone/stats#/100/team/61635/stats
BUF = pd.read_csv(DATA_DIR + '/buf.csv')
# https://whale.nwhl.zone/stats#/100/team/61637/stats
CON = pd.read_csv(DATA_DIR + '/con.csv')
# https://riveters.nwhl.zone/stats#/100/team/61636/stats
MET = pd.read_csv(DATA_DIR + '/met.csv')
# https://whitecaps.nwhl.zone/stats#/100/team/61639/stats
MIN = pd.read_csv(DATA_DIR + '/min.csv')
# http://toronto.nwhl.zone/stats#/100/team/79963/stats
TOR = pd.read_csv(DATA_DIR + '/tor.csv')

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#
#                             NWHL FUNCTIONS
#
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

def clean_teamstats(team_sheet, team_abbreviation):
    #---------------------------------------------------------------------------
    # Clean Team Stats Sheet
    #---------------------------------------------------------------------------
    team_sheet['Team'] = team_abbreviation
    
    team_sheet = team_sheet.rename(columns = {
        '#': 'Number',
        'Pos': 'Position',
        'GP': 'GamesPlayed',
        'G': 'Goals_Total',
        'A': 'Assists_Total',
        'PTS': 'Points_Total',
        'SOG': 'Shots_OnGoal',
        '+/-': 'PlusMinus_Total',
        'BkS': 'Blocked_Shots',
        'PIM': 'Penalty_Minutes',
        'FO W/L': 'Faceoffs_WinLoss', 
        'TkA': 'Takeaways',
        'GvA': 'Giveaways',
        'S%': 'Shots_GoalsPercent',
        'PPG': 'Goals_PowerPlay',
        'SHG': 'Goals_ShortHanded',
        'GWG': 'Goals_GameWinning',
        'PPGA': 'Points_PerGame',
        'FO%': 'Faceoffs_Percent',
        'ShB': 'Shots_Blocked',
        'Sh': 'Shots_Total'
        })

    #---------------------------------------------------------------------------
    # Clean Faceoffs
    #---------------------------------------------------------------------------
    team_sheet['Faceoffs_Won'] = team_sheet.Faceoffs_WinLoss.apply(lambda x: pd.to_numeric(x.split()[0]))
    team_sheet['Faceoffs_Lost'] = team_sheet.Faceoffs_WinLoss.apply(lambda x: pd.to_numeric(x.split()[2]))
    team_sheet['Faceoffs_Total'] = team_sheet['Faceoffs_Won'] + team_sheet['Faceoffs_Lost']
    team_sheet = team_sheet.drop(['Faceoffs_WinLoss'], axis=1)
    team_sheet.Faceoffs_Percent = team_sheet.Faceoffs_Percent.apply(lambda x: round(x,2))

    #---------------------------------------------------------------------------
    # Relative Plus-Minus
    #---------------------------------------------------------------------------
    Team_PlusMinus = team_sheet.PlusMinus_Total.mean()
    team_sheet['PlusMinus_Relative'] = team_sheet.PlusMinus_Total - Team_PlusMinus
    team_sheet['PlusMinus_Relative'] = team_sheet['PlusMinus_Relative'].astype('int')

    #---------------------------------------------------------------------------
    # Clean Shots
    #---------------------------------------------------------------------------
    team_sheet['Shots_BlockedPercent'] = team_sheet.Shots_Blocked / team_sheet.Shots_Total
    team_sheet['Shots_BlockedPercent'] = team_sheet['Shots_BlockedPercent'].astype('float')
    team_sheet.Shots_BlockedPercent = team_sheet.Shots_BlockedPercent.apply(lambda x: round(x,2))

    team_sheet['Shots_Missed'] = team_sheet.Shots_Total - team_sheet.Shots_OnGoal - team_sheet.Shots_Blocked
    team_sheet['Shots_Missed'] = team_sheet['Shots_Missed'].astype('int')

    team_sheet['Shots_MissedPercent'] = team_sheet.Shots_Missed / team_sheet.Shots_Total
    team_sheet['Shots_MissedPercent'] = team_sheet['Shots_MissedPercent'].astype('float')
    team_sheet.Shots_MissedPercent = team_sheet.Shots_MissedPercent.apply(lambda x: round(x,2))

    team_sheet['Shots_OnGoalPercent'] = team_sheet.Shots_OnGoal / team_sheet.Shots_Total
    team_sheet['Shots_OnGoalPercent'] = team_sheet['Shots_OnGoalPercent'].astype('float')
    team_sheet.Shots_OnGoalPercent = team_sheet.Shots_OnGoalPercent.apply(lambda x: round(x,2))

    #---------------------------------------------------------------------------
    # Turnover Plus Minus
    #---------------------------------------------------------------------------
    team_sheet['PlusMinus_Turnovers'] = team_sheet.Takeaways - team_sheet.Giveaways
    team_sheet['PlusMinus_Turnovers'] = team_sheet['PlusMinus_Turnovers'].astype('int')

    #---------------------------------------------------------------------------
    # Penalties Per Game
    #---------------------------------------------------------------------------
    team_sheet['Penalty_Minutes_PerGame'] = team_sheet.Penalty_Minutes / team_sheet.GamesPlayed
    team_sheet['Penalty_Minutes_PerGame'] = team_sheet['Penalty_Minutes_PerGame'].astype('float')
    team_sheet.Penalty_Minutes_PerGame = team_sheet.Penalty_Minutes_PerGame.apply(lambda x: round(x,2))

    #---------------------------------------------------------------------------
    # Blocked Shots Per Game
    #---------------------------------------------------------------------------
    team_sheet['Blocked_Shots_PerGame'] = team_sheet.Blocked_Shots / team_sheet.GamesPlayed
    team_sheet['Blocked_Shots_PerGame'] = team_sheet['Blocked_Shots_PerGame'].astype('float')
    team_sheet.Blocked_Shots_PerGame = team_sheet.Blocked_Shots_PerGame.apply(lambda x: round(x,2))

    #---------------------------------------------------------------------------
    # Shots Per Game
    #---------------------------------------------------------------------------
    team_sheet['Shots_PerGame'] = team_sheet.Shots_Total / team_sheet.GamesPlayed
    team_sheet['Shots_PerGame'] = team_sheet['Shots_PerGame'].astype('int')

    #---------------------------------------------------------------------------
    # Percent of Team Faceoffs
    #---------------------------------------------------------------------------
    Team_Faceoffs = team_sheet.Faceoffs_Total.sum()
    team_sheet['Faceoffs_TeamPercent'] = team_sheet.Faceoffs_Total / Team_Faceoffs
    team_sheet['Faceoffs_TeamPercent'] = team_sheet['Faceoffs_TeamPercent'].astype('float')
    team_sheet.Faceoffs_TeamPercent = team_sheet.Faceoffs_TeamPercent.apply(lambda x: round(x,2))

    #---------------------------------------------------------------------------
    # Clean Columns
    #---------------------------------------------------------------------------
    team_sheet = team_sheet[[
        'Name',
        'Number',
        'Position',
        'Team',
        'GamesPlayed',
        'Goals_Total',
        'Goals_PowerPlay',
        'Goals_ShortHanded',
        'Goals_GameWinning',
        'Assists_Total',
        'Points_Total',
        'Points_PerGame',
        'Shots_OnGoal',
        'Shots_OnGoalPercent',
        'Shots_Blocked',
        'Shots_BlockedPercent',
        'Shots_Missed',
        'Shots_MissedPercent',
        'Shots_Total',
        'Shots_PerGame',
        'Shots_GoalsPercent',
        'PlusMinus_Total',
        'PlusMinus_Relative',
        'Penalty_Minutes',
        'Penalty_Minutes_PerGame',
        'Blocked_Shots',
        'Blocked_Shots_PerGame',
        'Takeaways',
        'Giveaways',
        'PlusMinus_Turnovers',
        'Faceoffs_Won',
        'Faceoffs_Lost',
        'Faceoffs_Total',
        'Faceoffs_Percent',
        'Faceoffs_TeamPercent',
    ]]
    
    return team_sheet

bos_clean = clean_teamstats(BOS, 'Boston Pride')
buf_clean = clean_teamstats(BUF, 'Buffalo Beauts')
con_clean = clean_teamstats(CON, 'Connecticut Whale')
met_clean = clean_teamstats(MET, 'Metropolitan Riveters')
min_clean = clean_teamstats(MIN, 'Minnesota Whitecaps')
# tor_clean = clean_teamstats(TOR, 'Toronto')

nwhl_list = []
nwhl_list.append(bos_clean)
nwhl_list.append(buf_clean)
nwhl_list.append(con_clean)
nwhl_list.append(met_clean)
nwhl_list.append(min_clean)
# nwhl_list.append(tor_clean)

nwhl_clean = pd.concat(nwhl_list)
