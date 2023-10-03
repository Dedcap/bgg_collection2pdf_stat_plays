#!/usr/bin/env python3

import requests
import textwrap
import shutil
import argparse
import os
import sys
from time import sleep
from xml.etree import ElementTree
import logging
from urllib.parse import urlencode, quote
from datetime import datetime
import contextlib

import math

import pandas as pd 
import pandas.io.formats.excel
import xlsxwriter

import matplotlib.pyplot as plt
starttime = datetime.now()
######### Begin Classes #########

class config:
    def __init__(self, args):
        self.LOGLEVEL                = os.environ.get('LOGLEVEL', 'INFO').upper()
        self.bgg                     = 'https://boardgamegeek.com/xmlapi2'
        self.successful_responses    = 0
        self.dict_player_count       = {}
        self.dict_category           = {}
        self.dict_game_info           = {}
        self.dict_plays_info           = {}

        self.user_name               = args.username
        self.card_mode               = args.cardmode or False
        self.index                   = args.index    or False
        self.only_own                = args.own      or False
        self.plays                   = args.plays    or False
        self.want_to_play            = args.want_to_play or False

        self.template                = "./template.html"
        self.card_template           = "./template_card.html"
        self.plays_template          = "./template_plays.html"
        self.not_play_template       = "./template_plays.html"
        self.images_template         = "./Images-templates"

        self.output                  = args.output if len(args.output) > 0 else"./output.html"
        self.output_plays            = args.output_plays if len(args.output_plays) > 0 else"./output_plays.html"
        self.output_not_play         = args.output_not_play if len(args.output_not_play) > 0 else"./output_not_play.html"
        self.output_xlsx             = args.output_xlsx if len(args.output_xlsx) > 0 else"./Plays.xlsx"
        self.collection_xml          = args.collection_xml if len(args.collection_xml) > 0 else"./collection.xml"
        self.images_path             = args.images_path if len(args.images_path) > 0 else"./Images"
        self.xml_path                = args.xml_path if len(args.xml_path) > 0 else"./game_xml"

        self.sleep_time              = int(args.minsleep) if len(args.minsleep) > 0 else 10
        self.sleep_time_max          = int(args.maxsleep) if len(args.maxsleep) > 0 else 120
        self.no_cache                = args.no_cache or False
        self.no_cache_plays          = args.no_cache_plays or False
        self.web_mode                = os.path.exists("./app.py")

class collection_information:
    def __init__(self, item, config):
        self.obj_id     = item.attrib['objectid']
        self.game_name  = item.find('name').text
        self.game_xml   = os.path.join(config.xml_path, self.obj_id + '.xml')
        self.own        = item.find('status').attrib['own'] == "1"
        self.want_to_play  = item.find('status').attrib['wanttoplay'] == "1"
        self.num_plays  = item.find('numplays').text
        self.my_rating  = item.find('stats').find('rating').attrib['value']
        self.avg_rating = item.find('stats').find('rating').find('average').attrib['value']
        self.my_image   = item.find('image').text if item.find('image') != None else ""

class game_information:
    def __init__(self, items, config, collection_info):
        self.image                  = collection_info.my_image if collection_info.my_image != "" else get_prop_text(items, 'image')
        self.name                   = get_prop_value(items, 'name')
        self.obj_id                 = collection_info.obj_id
        self.my_rating              = collection_info.my_rating
        self.avg_rating             = collection_info.avg_rating
        self.minplayers             = str(get_prop_value(items, 'minplayers') or '')
        self.maxplayers             = str(get_prop_value(items, 'maxplayers') or '')
        self.published              = get_prop_value(items, 'yearpublished')
        self.num_plays              = collection_info.num_plays
        self.publisher              = get_value_in_list(get_links(items, 'boardgamepublisher'), 0)
        self.designer               = get_value_in_list(get_links(items, 'boardgamedesigner'), 0)
        self.artist1                = get_value_in_list(get_links(items, 'boardgameartist'), 0)
        self.artist2                = get_value_in_list(get_links(items, 'boardgameartist'), 1)
        self.category1              = get_value_in_list(get_links(items, 'boardgamecategory'), 0)
        self.category2              = get_value_in_list(get_links(items, 'boardgamecategory'), 1)
        self.mechanic1              = get_value_in_list(get_links(items, 'boardgamemechanic'), 0)
        self.mechanic2              = get_value_in_list(get_links(items, 'boardgamemechanic'), 1)
        self.mechanic3              = get_value_in_list(get_links(items, 'boardgamemechanic'), 2)
        self.mechanic4              = get_value_in_list(get_links(items, 'boardgamemechanic'), 3)
        self.mintime                = str(get_prop_value(items, 'minplaytime') or '')
        self.maxtime                = str(get_prop_value(items, 'maxplaytime') or '')
        self.avg_weight             = items.find('statistics').find('ratings').find('averageweight').attrib['value']
        self.three_mechanics_length = len((self.mechanic1 or "") + (self.mechanic2 or "") + (self.mechanic3 or ""))
        self.four_mechanics_length  = len((self.mechanic1 or "") + (self.mechanic2 or "") + (self.mechanic3 or "") + (self.mechanic4 or ""))
        self.description            = textwrap.shorten(get_prop_text(items, 'description') or "", width=get_description_length(config), placeholder='...')
        self.lastPlayed             = ""

######### End Classes #########

######### Begin Functions #########

#command is an api command from BGG (user, collection, etc)
#params is a dictionary with parameter/value pairs for the command
def bgg_getter (command, params, config):
    sleep(.3)
    status = 0
    a = ''
    while not status == 200:
        url = '{}/{}?{}'.format(config.bgg,
                                quote(command),
                                urlencode(params),
                                )
        logging.debug(url)
        a = requests.get(url)
        status = a.status_code
        if(status != 200):
            error = ElementTree.fromstring(a.content)
            try:
                err_msg = error.find('message').text
            except:
                err_msg = "HTTP Status " + str(status)
            logging.info("Sleeping " + str(config.sleep_time) + " Seconds: " + (err_msg))
            sleep(config.sleep_time)
            config.sleep_time *= 2
            config.sleep_time = min(config.sleep_time_max,config.sleep_time)
            config.successful_responses = 0
        else:
            config.successful_responses += 1
            if(config.successful_responses % 15 == 0):
                config.sleep_time /= 2
                config.sleep_time = max(10,config.sleep_time)
    return a

def parse_arguments():
    parser = argparse.ArgumentParser(description='Create an html/pdf output of board game collection based on UserName from boardgamegeek.com.')
    parser.add_argument('-u','--username', dest='username', action='store', default='', help='User to pull BGG collection data from. (Required)')
    parser.add_argument('-c','--cardmode', dest='cardmode', action='store_true', help='Create cards instead of a catalog. (default=Off)')
    parser.add_argument('-i','--index', dest='index', action='store_true', help='Enables creating an index. (default=Off)')
    parser.add_argument('-pl','--plays', dest='plays', action='store_true', help='Defining if the plays stored in BBG will be retrieved and proceed. (default=Off)')
    parser.add_argument('--clean_images', dest='clean_images', action='store_true', help='Clear out local images cache. (default=Off)')
    parser.add_argument('--clean_xml', dest='clean_xml', action='store_true', help='Clear out local xml cache. (default=Off)')
    parser.add_argument('--clean_plays', dest='clean_plays', action='store_true', help='Clear out Excel file storing the plays data (default=Off)')
    parser.add_argument('--clean_all', dest='clean_all', action='store_true', help='Clear out Images, XML, and all other generated files (default=Off)')
    parser.add_argument('-o','--own',dest='own', action='store_true', help='Enables pulling only games set to own on BGG. (default=Off)')
    parser.add_argument('-wtp','--want_to_play',dest='want_to_play', action='store_true', help='Enables pulling only games set to Want to play on BGG. (default=Off)')
    parser.add_argument('--minsleep', dest='minsleep', action='store', default='', help='Minimum sleep duration on XML error. (Default=10)')
    parser.add_argument('--maxsleep', dest='maxsleep', action='store', default='', help='Maximum sleep duration on XML error. (Default=120)')
    parser.add_argument('--output', dest='output', action='store', default='', help='Output html file. (Default="./output.html")')
    parser.add_argument('--output_plays', dest='output_plays', action='store', default='', help='Output html file for plays. (Default="./output_plays.html")')
    parser.add_argument('--output_not_play', dest='output_not_play', action='store', default='', help='Output html file for game not plays this year. (Default="./output_not_play.html")')
    parser.add_argument('--output_xlsx', dest='output_xlsx', action='store', default='', help='Output Excel file for plays. (Default="./Plays.xlsx")')
    parser.add_argument('--images_path', dest='images_path', action='store', default='', help='Images path. (Default="./Images")')
    parser.add_argument('--xml_path', dest='xml_path', action='store', default='', help='Game XML Path. (Default="./game_xml")')
    parser.add_argument('--collection_xml', dest='collection_xml', action='store', default='', help='Output collection XML file.(Default="./collection.xml")')
    parser.add_argument('--no_cache', dest='no_cache', action='store_true', help='Turn off all caching (default=Off)')
    parser.add_argument('--no_cache_plays', dest='no_cache_plays', action='store_true', help='Turn off caching for registered plays (default=Off)')
    return parser.parse_args()

def get_value(item):
    return item.attrib['value']

def get_value_in_list(item, i):
    if(len(item) <= i):
        return None
    else:
        return item[i].attrib['value']

def get_prop_text(elem, name):
    elem = elem.find(name)
    if elem is not None:
        return elem.text

def get_prop_value(elem, name):
    elem = elem.find(name)
    if elem is not None:
        return get_value(elem)

def get_links(elem, name):
    values = []
    elem = elem.findall('link')
    for item in elem:
        if (item.attrib['type'] == name):
            if item is not None:
                values.append(item)
    return values

def open_template(config):
    if(config.card_mode):
        with open(config.card_template, 'r') as file:
            return file.read()
    else:
        with open(config.template, 'r') as file:
            return file.read()

def open_plays_template(config):
    with open(config.plays_template, 'r') as file:
        return file.read()

def open_not_play_template(config):
    with open(config.not_play_template, 'r') as file:
        return file.read()

def get_mechanics_list_max_length(config):
    if(config.card_mode):
        return 65
    else:
        return 75

def get_description_length(config):
    if(config.card_mode):
        return 450
    else:
        return 1000

def template_to_output_entry(config, game_info):
    mechanics_list_max_length = get_mechanics_list_max_length(config)

    #Read the template.
    template = open_template(config)

    #Replace values in the template.
    if(config.no_cache):
        template = template.replace('{{image}}'     , game_info.image or "")
    else:
        template = template.replace('{{image}}'     , os.path.join(config.images_path, game_info.obj_id + ".jpg") or "")

    template = template.replace('{{GameName}}'      , game_info.name                            or "N/A")
    template = template.replace('{{Description}}'   , game_info.description                     or "N/A")
    template = template.replace('{{Published}}'     , game_info.published                       or "N/A")
    template = template.replace('{{Publisher}}'     , game_info.publisher                       or "N/A")
    template = template.replace('{{Designer}}'      , game_info.designer                        or "N/A")
    template = template.replace('{{Artist}}'        , game_info.artist1                         or "N/A")
    template = template.replace('{{Category}}'      , (game_info.category1                      or "") + "<br/>" + (game_info.category2 or ""))
    template = template.replace('{{Numplays}}'      , game_info.num_plays                       or "N/A")

    if (mechanics_list_max_length >= game_info.four_mechanics_length):
        mechanics = [game_info.mechanic1, game_info.mechanic2, game_info.mechanic3, game_info.mechanic4]
    elif (mechanics_list_max_length >= game_info.three_mechanics_length):
        mechanics = [game_info.mechanic1, game_info.mechanic2, game_info.mechanic3]
    else:
        mechanics = [game_info.mechanic1, game_info.mechanic2]

    template = template.replace('{{Mec}}', ",".join(item for item in mechanics if item))
    template = template.replace('{{p}}'             , game_info.minplayers + " - " + game_info.maxplayers)
    template = template.replace('{{d}}', str(game_info.mintime) + " - " + str(game_info.maxtime) if (int(game_info.mintime) < int(game_info.maxtime)) else str(game_info.mintime))
    template = template.replace('{{Weight}}'        , str(round(float(game_info.avg_weight) * 2, 1) )) #Weight is doubled to be on the same scale with rating.
    template = template.replace('{{Rating}}', str(round(float(game_info.avg_rating), 1)) if ("N/A" in game_info.my_rating) else str(round((float(game_info.avg_rating) + float(game_info.my_rating)) / 2, 1)))
    
    template = template.replace('{{LastPlayed}}'      , game_info.lastPlayed                       or "N/A")

    #Write to output.html
    with open(config.output, 'a', encoding="utf-8") as file:
        file.write(template)

def download_image(config, game_info):
    if not (config.no_cache):
        #If we have a local cache of the image, then don't try to redownload it, use the local copy.
        if(os.path.exists(os.path.join(config.images_path, game_info.obj_id + ".jpg")) == False):
            #Download the image to the local cache.
            res = requests.get(game_info.image, stream = True)
            if res.status_code == 200:
                logging.info("Writing: " + collection_info.game_name + " boxart to " + os.path.join(config.images_path, game_info.obj_id + ".jpg"))
                with open(os.path.join(config.images_path, game_info.obj_id + ".jpg"), 'wb') as f:
                    shutil.copyfileobj(res.raw, f)

def break_if_required(file, line_text, do_break):
    if(do_break):
        file.write("</ul>\n")
        file.write('<p style="page-break-after: always;"></p>\n')
        file.write("<ul>\n")
        if(len(line_text) > 0):
            file.write("<br><li><b>" + line_text + "</b></li>\n")

def write_error_to_output_html_and_close(config, error):
    write_output_header(config)
    with open(config.output, 'w') as file:  
        file.write(error)
    write_output_trailer(config.output)
    sys.exit()

def validate_username(config):
    validUserName   = False
    while not validUserName:
        thisdata = bgg_getter('user', {'name': config.user_name}, config)
        root = ElementTree.fromstring(thisdata.content)
        if root.attrib['id']:
            validUserName = True
            logging.info(f'UserName: {config.user_name} is valid')
        else:
            logging.warning(f'UserName: {config.user_name} was not valid')
            write_error_to_output_html_and_close(config, f'UserName: {config.user_name} was not valid')
    return config.user_name

def clean_up(config):
    if args.clean_images or args.clean_xml or args.clean_all:
        logging.info('Cleaning...')
        if args.clean_images or args.clean_all:
            for f in os.listdir(config.images_path):
                if(os.path.exists(os.path.join(config.images_path, f))):
                    if(f == 'icon_players.png' or f == 'icon_duration.png'):
                        continue
                    os.remove(os.path.join(config.images_path, f))
        if args.clean_xml or args.clean_all:
            if(os.path.exists(config.collection_xml)):
                os.remove(config.collection_xml)
            for f in os.listdir(config.xml_path):
                if(os.path.join(config.xml_path, f)):
                    os.remove(os.path.join(config.xml_path, f))
        if args.clean_plays or args.clean_all:
            if(os.path.exists(config.output_xlsx)):
                os.remove(config.output_xlsx)
        if args.clean_all:
            with contextlib.suppress(FileNotFoundError):
                os.remove(config.output)
                os.remove(config.output_plays)
                os.remove(config.output_not_play)
                os.remove(config.collection_xml)
        sys.exit()

def write_output_header(config):
    with open(config.output, 'w') as file:      
        if(config.web_mode):
            if(config.card_mode):
                file.write('<html><head><link href="{{ url_for(\'static\', filename=\'styles/style_card.css\')}}" rel="stylesheet" type="text/css"></head><body>')
            else:
                file.write('<html><head><link href="{{ url_for(\'static\', filename=\'styles/style.css\')}}" rel="stylesheet" type="text/css"></head><body>')
        else:
            if(config.card_mode):
                file.write('<html><head><link href="style_card.css" rel="stylesheet" type="text/css"></head><body>')
            else:
                file.write('<html><head><link href="style.css" rel="stylesheet" type="text/css"></head><body>')

def write_output_plays_header(config):
    with open(config.output_plays, 'w') as file:      
        if(config.web_mode):
            file.write('<html><head><link href="{{ url_for(\'static\', filename=\'styles/style_plays.css\')}}" rel="stylesheet" type="text/css"></head><body>')
        else:
            file.write('<html><head><link href="style_plays.css" rel="stylesheet" type="text/css"></head><body>')

def write_output_not_play_header(config):
    with open(config.output_not_play, 'w') as file:      
        if(config.web_mode):
            file.write('<html><head><link href="{{ url_for(\'static\', filename=\'styles/style_plays.css\')}}" rel="stylesheet" type="text/css"></head><body>')
        else:
            file.write('<html><head><link href="style_plays.css" rel="stylesheet" type="text/css"></head><body>')

def request_collection(config):        
    logging.warning('Reading collection from bgg')

    status = 0
    params = {'username': config.user_name, 'stats': 1}

    if config.only_own:
        params['own'] = 1
        
    if config.want_to_play:
        params['wanttoplay'] = 1
    
    collection_response = bgg_getter('collection',params, config)
    with open(config.collection_xml, 'w', encoding="utf-8") as file:
        file.write(collection_response.text)
        return ElementTree.fromstring(collection_response.content)

def read_collection(config):
    if not (config.no_cache):
        #Check if collection.xml exists. If it does, read it.
        if(os.path.exists(config.collection_xml)):
            logging.warning('Reading ' + config.collection_xml)
            with open(config.collection_xml, 'r', encoding="utf-8") as file:
                return ElementTree.fromstring(file.read())

        #Otherwise we request the XML from BGG
        else:
          return request_collection(config)  

    #Otherwise we request the XML from BGG
    else:
        return request_collection(config)  

def download_and_split_collection_object_info(config, newids):
    newgamexmls = bgg_getter('thing', {'id': ','.join(newids), 'stats': 1}, config)
    for item in ElementTree.fromstring(newgamexmls.content):
        if not (config.no_cache):
            game_xml_path = os.path.join(config.xml_path, item.attrib['id'] + '.xml')
            with open(game_xml_path, 'w', encoding='utf-8') as file:
                logging.info(f'Writing to {game_xml_path}')
                file.write(ElementTree.tostring(item, encoding='unicode'))
        else:
            config.dict_game_info[item.attrib['id']] = item

def download_and_store_plays_object_info(config, newid):
    global playsArrays
    
    newgameplaysxmls = bgg_getter('plays', {'id': str(newid), 'type': 'thing', 'username': config.user_name}, config)
    
    lastPlayed = ""
    
    for item in ElementTree.fromstring(newgameplaysxmls.content):
        playersItems = item.find('players')
        if playersItems is not None:
            players = playersItems.findall('player')
            for player in players:
                if player is not None:
                    playsArrays.append([newid,
                            item.find('item').attrib['name'],
                            item.attrib['id'],
                            item.attrib['date'],
                            item.attrib['quantity'],
                            player.attrib['name'],
                            player.attrib['win']])
                    
                    if (item.attrib['date'] > lastPlayed):
                        lastPlayed = item.attrib['date']
    
    return lastPlayed

def find_and_download_new_collection_object_info(config, collection):
    newids = set()
    for item in collection:
        collection_info = collection_information(item, config)
        if not (os.path.exists(collection_info.game_xml)):
            newids.add(collection_info.obj_id)
            logging.debug(f'Adding ID: {collection_info.obj_id} for download')
        else:
            logging.debug(f'Skipping ID: {collection_info.obj_id} for download')
        if len(newids)>100:
            logging.debug(f'Collected 100 ids - passing for download')
            download_and_split_collection_object_info(config, newids)
            newids = set()
    if newids:
        logging.debug(f'Downloading remaining new ids')
        download_and_split_collection_object_info(config, newids)
        

def gather_index_info(config, gameinfo, item):
    for count in range(int(gameinfo.minplayers), int(gameinfo.maxplayers)):
        if(count not in config.dict_player_count):
            config.dict_player_count[count] = []
        config.dict_player_count[count].append(gameinfo)

    for x in get_links(item, 'boardgamecategory'):
        category = x.attrib['value']
        if(category not in config.dict_category):
            config.dict_category[category] = []
        config.dict_category[category].append(gameinfo)

def write_index(config):
    if(config.index):

        with open(config.output, 'a') as file:
            i = 1
            break_point = 250

            file.write('<p style="page-break-after: always;"></p>\n')
            file.write("<ul>\n")
            for count in range(1,10):
                file.write("<br><li><b>" + str(count) + " player games:" + "</b></li>\n")
                i += 1
                break_if_required(file, "",i % break_point == 0)
                for game in config.dict_player_count[count]:
                    file.write("<li>" + game.name + "</li>\n")
                    i += 1
                    break_if_required(file, str(count) + " player games:", i % break_point == 0)
            file.write("</ul>\n")

            i = 1
            break_point = 250

            file.write('<p style="page-break-after: always;"></p>\n')
            file.write('<ul>\n')
            for cat in sorted(config.dict_category):
                file.write("<br><li><b>" + str(cat) + " games:" + "</b></li>\n")
                i += 1
                break_if_required(file, "", i % break_point == 0)
                for game in config.dict_category[cat]:
                    file.write("<li>" + game.name + "</li>\n")
                    i += 1
                    break_if_required(file, str(cat) + " games:", i % break_point == 0)
            file.write("</ul>\n")

def write_output_trailer(outputFileName):
    #Write the html trailer.
    with open(outputFileName, 'a') as file:
            file.write("</body></html>")

def getImage(path, zoom=1):
    image_resized = resize(plt.imread(path), (50, 50), anti_aliasing=True)
    #image_downscaled = downscale_local_mean(plt.imread(path), (4, 3))
    #return OffsetImage(plt.imread(path), zoom=zoom)
    return OffsetImage(image_resized, zoom=zoom)
    #return OffsetImage(image_downscaled, zoom=zoom)

#image_rescaled = rescale(image, 0.25, anti_aliasing=False)
#image_resized = resize(image, (image.shape[0] // 4, image.shape[1] // 4),
#                       anti_aliasing=True)
#image_downscaled = downscale_local_mean(image, (4, 3))

def xlxs_size(worksheet): # output the size of the written data in an excel sheet : max number of row, max number of column (starting with 0)
    return worksheet.dim_rowmax,worksheet.dim_colmax

def write_plays_excelfile(config, current_year, playsDF, lastPlays2023DF, lastPlaysBefore2023DF, groupedPlaysPerGame2023DF, groupedPlaysPerGameDF, groupedVictoryPlays2023DF, groupedVictoryPlaysDF, groupedPlayerDF, groupedPlaysPerGamePerPlayer2023DF): #Saving the various dataframe about registered plays to an Excel file
    pandas.io.formats.excel.ExcelFormatter.header_style = None
    excel = pd.ExcelWriter(config.output_xlsx,datetime_format='dd/mm/yyyy',date_format='dd/mm/yyyy')
    workbook = excel.book
    titleFormat = workbook.add_format({'bg_color': '#DDEBF7','bold':True})
    
    store_spreadsheet(playsDF, titleFormat, 'Main', excel)
    store_spreadsheet(lastPlays2023DF, titleFormat, current_year+'_Last_Plays', excel)
    store_spreadsheet(lastPlaysBefore2023DF, titleFormat, 'Bef_'+current_year+'_Last_Plays', excel)
    store_spreadsheet(groupedPlaysPerGame2023DF, titleFormat, current_year+'_PlaysPerGame', excel)
    store_spreadsheet(groupedPlaysPerGameDF, titleFormat, 'AllYear_PlaysPerGame', excel)
    store_spreadsheet(groupedVictoryPlays2023DF, titleFormat, current_year+'_Victory_Plays', excel)
    store_spreadsheet(groupedVictoryPlaysDF, titleFormat, 'AllYear_Victory_Plays', excel)
    store_spreadsheet(groupedPlayerDF, titleFormat, 'Player_Count', excel)
    store_spreadsheet(groupedPlaysPerGamePerPlayer2023DF, titleFormat, current_year+'_PlaysPerPlayerPerGame', excel)
    
    try:
        excel.save()
    except Exception as e:
        print(str(e))

def store_spreadsheet(dataFrame, titleFormat, spreadsheetTitle, excel): # Attaching dataframe data into an Excel spreadsheet
    dataFrame.to_excel(excel, sheet_name=spreadsheetTitle,index=False)
    ws = excel.sheets[spreadsheetTitle]
    row,col = xlxs_size(ws)
    ws.autofilter(0,0,row,col)
    ws.set_row(0, 20.14, titleFormat)

######### End Functions #########

global playsArrays

#Get arguments.
args = parse_arguments()

#Create config.
config = config(args)

#Set loging level.
logging.basicConfig(level=config.LOGLEVEL)

#Cleanup if args set.
clean_up(config)

# Create the XML path if it does not exist.
os.makedirs(config.xml_path, exist_ok=True)

#Validate the username
config.user_name = validate_username(config)

logging.info('starting')

#Write the html header and link to the approprate CSS file.
write_output_header(config)

#Read in the collection xml file.
items = read_collection(config)

find_and_download_new_collection_object_info(config, items)

data = []
playsArrays = []

if(config.plays and os.path.exists(config.output_xlsx) and not config.no_cache_plays):#Reading existing Excel file where registered plays are stored
    print("Reading plays file")
    xlObjectPlays = pd.ExcelFile(config.output_xlsx)
    playsDF = pd.read_excel(xlObjectPlays, sheet_name="Main")
    lastPlaysDF = playsDF[['Id_Game', 'Name', 'Date']].pivot_table(index=['Id_Game', 'Name'], values='Date', aggfunc='max').reset_index()
#End of If

#Parsing user collection XML
for item in items:
    collection_info = collection_information(item, config)

    #Grab only games we own unless own isn't set.
    if(config.only_own == False or collection_info.own):
        #Check to see if the XML already exists. If it does, don't re-request it.
        if(os.path.exists(collection_info.game_xml) and not config.no_cache):
            with open(collection_info.game_xml, 'r', encoding="utf-8") as file:
                thisgameitems = ElementTree.fromstring(file.read())
        elif not (config.no_cache):
                logging.error('game not found')
                #Pull the game info XML
                game_info_response = bgg_getter('thing', {'id': collection_info.obj_id, 'stats': 1} , config)
                
                #Write out the game info XML.
                with open(collection_info.game_xml, 'w', encoding="utf-8") as file:
                    logging.info("Writing: " + collection_info.game_name + " to " + collection_info.game_xml)
                    file.write(game_info_response.text)
                    thisgameitems = ElementTree.fromstring(game_info_response.content)
        else:
            thisgameitems = config.dict_game_info[collection_info.obj_id]

        #Now that we have all of the information we need, create the HTML page.
        if(thisgameitems.attrib['type'] == "boardgame"):
            game_info = game_information(thisgameitems, config, collection_info)
            download_image(config, game_info)
            
            if(config.plays and os.path.exists(config.output_xlsx) and not config.no_cache_plays):
                lastPlayedRow = lastPlaysDF.loc[lastPlaysDF['Id_Game'] == int(game_info.obj_id)]
                if (len(lastPlayedRow.index) >=1):
                    lastPlayed = str(lastPlayedRow['Date'].values[0])
                else:
                    lastPlayed = "N/A"
                
                game_info.lastPlayed = lastPlayed
            elif (config.plays):
                lastPlayed = download_and_store_plays_object_info(config, game_info.obj_id)
                game_info.lastPlayed = lastPlayed
            else :
                lastPlayed = "N/A"
            
            template_to_output_entry(config, game_info)
            gather_index_info(config, game_info, thisgameitems)
            
            
            data.append([game_info.obj_id,
                            game_info.name,
                            float(game_info.mintime),
                            float(game_info.maxtime),
                            float(game_info.avg_weight),
                            str(os.path.join(config.images_path, game_info.obj_id + ".jpg"))])
            
        else:
            expName = get_prop_value(thisgameitems, 'name')
            expansion = thisgameitems.attrib['type']
            logging.info(f'Expansion: {expName}')
            logging.info(f'Expansion - type: {expansion}')



#Write the index.
write_index(config)

#Write the trailer.
write_output_trailer(config.output)

#If plays is not to be proceed, we end the script here
if not (config.plays):
    endtime = datetime.now()
    totaltime = endtime - starttime
    logging.info(f'command: {sys.argv}')
    logging.info(f'total time: {totaltime}')
    sys.exit()

if(os.path.exists(config.output_xlsx) and not config.no_cache_plays):
    print("Re-using loaded data for plays")
else:
    playsDF = pd.DataFrame(playsArrays)
    playsDF.columns = ['Id_Game', 'Name', 'Id_Play', 'Date', 'Quantity', 'Player_Name', 'Victory']
#End of IF    

# using dictionary to convert specific columns
convert_dict = {'Id_Game': int,
                    'Id_Play': int,
                    'Quantity': int,
                    'Victory': int
                    }
playsDF = playsDF.astype(convert_dict)

#Manage date and year for filter
starting_day_of_current_year = datetime.now().date().replace(month=1, day=1)  
current_year = str(starting_day_of_current_year.strftime("%Y"))
starting_day_of_current_year = str(starting_day_of_current_year.strftime("%Y-%m-%d"))
logging.info("Current year for processing games static: "+current_year)
logging.info("First day of the current year to split game static: "+starting_day_of_current_year)

#Filtering registered plays on the 2023 year
plays2023DF = playsDF.loc[(playsDF['Date'] >= starting_day_of_current_year)]

lastPlays2023DF = plays2023DF[['Id_Game', 'Name', 'Date']].pivot_table(index=['Id_Game', 'Name'], values='Date', aggfunc='max').reset_index()

lastPlaysBefore2023DF = playsDF[['Id_Game', 'Name', 'Date']].pivot_table(index=['Id_Game', 'Name'], values='Date', aggfunc='max').reset_index()
lastPlaysBefore2023DF = lastPlaysBefore2023DF.loc[(lastPlaysBefore2023DF['Date'] < starting_day_of_current_year)]

groupedPlaysPerGame2023IntermDF = plays2023DF[['Id_Game', 'Name', 'Id_Play', 'Quantity']].pivot_table(index=['Id_Game', 'Name', 'Id_Play', 'Quantity']).reset_index()
groupedPlaysPerGame2023DF = groupedPlaysPerGame2023IntermDF[['Id_Game', 'Name', 'Quantity']].pivot_table(index=['Id_Game', 'Name'], values='Quantity', aggfunc='sum').reset_index()

groupedPlaysPerGamePerPlayer2023DF = plays2023DF[['Id_Game', 'Name', 'Player_Name', 'Quantity']].pivot_table(index=['Id_Game', 'Name', 'Player_Name'], values='Quantity', aggfunc='sum').reset_index()
groupedPlaysPerGamePerPlayerDF = playsDF[['Id_Game', 'Name', 'Player_Name', 'Quantity']].pivot_table(index=['Id_Game', 'Name', 'Player_Name'], values='Quantity', aggfunc='sum').reset_index()

groupedPlaysPerGameIntermDF = playsDF[['Id_Game', 'Name', 'Id_Play', 'Quantity']].pivot_table(index=['Id_Game', 'Name', 'Id_Play', 'Quantity']).reset_index()
groupedPlaysPerGameDF = groupedPlaysPerGameIntermDF[['Id_Game', 'Name', 'Quantity']].pivot_table(index=['Id_Game', 'Name'], values='Quantity', aggfunc='sum').reset_index()

groupedVictoryPlays2023DF = plays2023DF[['Id_Game', 'Name', 'Player_Name', 'Quantity', 'Victory']].pivot_table(index=['Id_Game', 'Name', 'Player_Name', 'Quantity'], values='Victory', aggfunc='sum').reset_index()
groupedVictoryPlays2023DF["NB_VictoryInt"] = groupedVictoryPlays2023DF['Quantity']*groupedVictoryPlays2023DF['Victory']
groupedVictoryPlays2023DF = groupedVictoryPlays2023DF[['Id_Game', 'Name', 'Player_Name', 'NB_VictoryInt']].pivot_table(index=['Id_Game', 'Name', 'Player_Name'], values='NB_VictoryInt', aggfunc='sum').reset_index()

groupedVictoryPlaysDF = playsDF[['Id_Game', 'Name', 'Player_Name', 'Quantity', 'Victory']].pivot_table(index=['Id_Game', 'Name', 'Player_Name', 'Quantity'], values='Victory', aggfunc='sum').reset_index()
groupedVictoryPlaysDF["NB_VictoryInt"] = groupedVictoryPlaysDF['Quantity']*groupedVictoryPlaysDF['Victory']
groupedVictoryPlaysDF = groupedVictoryPlaysDF[['Id_Game', 'Name', 'Player_Name', 'NB_VictoryInt']].pivot_table(index=['Id_Game', 'Name', 'Player_Name'], values='NB_VictoryInt', aggfunc='sum').reset_index()


groupedPlayerDF = playsDF[['Player_Name', 'Victory']].pivot_table(index=['Player_Name'], values='Victory', aggfunc='sum').reset_index()
groupedPlayerDF = groupedPlayerDF.sort_values(by=['Victory'], ascending=False)


 
#Saving registered plays data into an Excel file
write_plays_excelfile(config, current_year, playsDF, lastPlays2023DF, lastPlaysBefore2023DF, groupedPlaysPerGame2023DF, groupedPlaysPerGameDF, groupedVictoryPlays2023DF, groupedVictoryPlaysDF, groupedPlayerDF, groupedPlaysPerGamePerPlayer2023DF)



meeplesAssociated = {}
meeplesAvailable = ["blue","yellow","green","pink","red","orange","black","violet"]

plt.rcParams.update({'font.size': 22})

#Proceed the association of Player with a color
for index, row in groupedPlayerDF.iterrows():
    playerName = str(row['Player_Name'])
    
    if (playerName not in meeplesAssociated):
        if (len(meeplesAvailable) >0):
            meepleColor = meeplesAvailable[0]
            meeplesAvailable.remove(meepleColor)
        else:
            meepleColor = "white"
            
        meeplesAssociated[playerName] = meepleColor
    #EndIf
#EndFor


#Write the html header and link to the approprate CSS file for the plays.
write_output_plays_header(config)


#Proceed the game plays this year
lastPlays2023DF = lastPlays2023DF.sort_values(by='Date', ascending=False)
for index, row in lastPlays2023DF.iterrows():
    #Read the template.
    template = open_plays_template(config)
    
    gameId = row['Id_Game']
    
    #Replace values in the template.
    if(config.no_cache):
        template = template.replace('{{image}}'     , game_info.image or "") ####game_info does not exist here bad copy paste
    else:
        template = template.replace('{{image}}'     , os.path.join(config.images_path, str(gameId) + ".jpg") or "")
    
    
    template = template.replace('{{GameId}}'      , str(gameId)                            or "")
    template = template.replace('{{GameName}}'      , row['Name']                            or "N/A")
    template = template.replace('{{LastPlayed}}'    , row['Date']                                or "N/A")
    
    nbPlays2023Row = groupedPlaysPerGame2023DF.loc[groupedPlaysPerGame2023DF['Id_Game'] == gameId]
    if (len(nbPlays2023Row.index) >=1):
        nbPlays2023 = str(nbPlays2023Row['Quantity'].values[0])
    else:
        nbPlays2023 = "N/A"
    template = template.replace('{{TP2023}}'        , nbPlays2023                               or "N/A")
    
    nbPlaysAllYearRow = groupedPlaysPerGameDF.loc[groupedPlaysPerGameDF['Id_Game'] == gameId]
    if (len(nbPlaysAllYearRow.index) >=1):
        nbPlaysAllYear = str(nbPlaysAllYearRow['Quantity'].values[0])
    else:
        nbPlaysAllYear = "N/A"
    template = template.replace('{{TPAll}}'         , nbPlaysAllYear                            or "N/A")
    
    
    groupedVictoryPlaysRows = groupedVictoryPlaysDF.loc[groupedVictoryPlaysDF['Id_Game'] == gameId]
    nbVictoryRows = groupedVictoryPlaysRows.loc[groupedVictoryPlaysRows['NB_VictoryInt'] > 0]
    if (len(nbVictoryRows.index) >= 1):
        groupedVictoryPlaysRows = groupedVictoryPlaysRows[['Player_Name', 'NB_VictoryInt']]
        pieData = groupedVictoryPlaysRows[['Player_Name', 'NB_VictoryInt']].pivot_table(index=['Player_Name'], values='NB_VictoryInt', aggfunc='sum').reset_index()
        pieDataSorted = pieData.sort_values(by=['NB_VictoryInt'], ascending=False)
        pieDataSorted.insert(1, "Color",pieDataSorted['Player_Name'].apply(lambda x: meeplesAssociated[x]))
        color_list = pieDataSorted['Color']
        pie = groupedVictoryPlaysRows[['Player_Name', 'NB_VictoryInt']].pivot_table(index=['Player_Name'], values='NB_VictoryInt', aggfunc='sum').sort_values(by=['NB_VictoryInt'], ascending=False).plot(kind="pie", y='NB_VictoryInt', ylabel='', colors=color_list, autopct=lambda p: format(p, '.1f') if p > 5 else None, subplots=True, legend = False)
        fig = pie[0].get_figure()
        fig.savefig(os.path.join(config.images_path, str(gameId) + "-result.png"), transparent=True)
        plt.cla()
        plt.close(fig)
        template = template.replace('{{victoryPie}}'     , os.path.join(config.images_path, str(gameId) + "-result.png") or "")
    else:
        template = template.replace('{{victoryPie}}'     , os.path.join(config.images_template, "looser-result.png") or "")
    
    results = ""
    victoryPlays2023Rows = groupedVictoryPlays2023DF.loc[groupedVictoryPlays2023DF['Id_Game'] == gameId]
    for vicIndex, victoryPlays2023Row in victoryPlays2023Rows.iterrows():
        playerName = str(victoryPlays2023Row['Player_Name'])
        
        if (playerName in meeplesAssociated):
            meepleColor = meeplesAssociated[playerName]
        else:
            if (len(meeplesAvailable) >0):
                meepleColor = meeplesAvailable[0]
                meeplesAvailable.remove(meepleColor)
            else:
                meepleColor = "white"
            
            meeplesAssociated[playerName] = meepleColor
        #EndIf
        
        results = results + "<b>"+playerName+"</b>: "
        victory = victoryPlays2023Row['NB_VictoryInt']
        groupedPlaysPerPlayerForSelectedGame2023DF = groupedPlaysPerGamePerPlayer2023DF.loc[groupedPlaysPerGamePerPlayer2023DF['Id_Game'] == gameId]
        nbPlaysRows = groupedPlaysPerPlayerForSelectedGame2023DF.loc[groupedPlaysPerPlayerForSelectedGame2023DF['Player_Name'] == playerName]
        players_nbPlays2023 = "999"
        if (len(nbPlaysRows.index) >=1):
            players_nbPlays2023 = str(nbPlaysRows['Quantity'].values[0])
        results = results + str(victory) + " / " + players_nbPlays2023 + " - "
        for x in range(0, victory):
            results = results + "<img src=\"" + os.path.join(config.images_template, "meeple-" + str(meepleColor) + ".png") + "\" class=\"meeple\"> "
        results = results + "<br>"
    #EndFor
    
    template = template.replace('{{Results}}'         , results                            or "N/A")
    
    #Write to output.html
    with open(config.output_plays, 'a', encoding="utf-8") as file:
        file.write(template)



#Write the trailer.
write_output_trailer(config.output_plays)



#Write the html header and link to the approprate CSS file for the plays.
write_output_not_play_header(config)

#Proceed the game not plays this year
lastPlaysBefore2023OrderedDF = lastPlaysBefore2023DF.sort_values(by='Date', ascending=False)
for index, row in lastPlaysBefore2023OrderedDF.iterrows():
    #Read the template.
    template = open_not_play_template(config)
    
    gameId = row['Id_Game']
    
    #Replace values in the template.
    if(config.no_cache):
        template = template.replace('{{image}}'     , game_info.image or "") ####game_info does not exist here bad copy paste
    else:
        template = template.replace('{{image}}'     , os.path.join(config.images_path, str(gameId) + ".jpg") or "")
    
    
    template = template.replace('{{GameId}}'      , str(gameId)                            or "")
    template = template.replace('{{GameName}}'      , row['Name']                            or "N/A")
    template = template.replace('{{LastPlayed}}'    , row['Date']                                or "N/A")
    template = template.replace('{{TP2023}}'        , "N/A")
    
    
    nbPlaysAllYearRow = groupedPlaysPerGameDF.loc[groupedPlaysPerGameDF['Id_Game'] == gameId]
    if (len(nbPlaysAllYearRow.index) >=1):
        nbPlaysAllYear = str(nbPlaysAllYearRow['Quantity'].values[0])
    else:
        nbPlaysAllYear = "N/A"
    template = template.replace('{{TPAll}}'         , nbPlaysAllYear                            or "N/A")
    
    
    groupedVictoryPlaysRows = groupedVictoryPlaysDF.loc[groupedVictoryPlaysDF['Id_Game'] == gameId]
    nbVictoryRows = groupedVictoryPlaysRows.loc[groupedVictoryPlaysRows['NB_VictoryInt'] > 0]
    if (len(nbVictoryRows.index) >= 1):
        groupedVictoryPlaysRows = groupedVictoryPlaysRows[['Player_Name', 'NB_VictoryInt']]
        pieData = groupedVictoryPlaysRows[['Player_Name', 'NB_VictoryInt']].pivot_table(index=['Player_Name'], values='NB_VictoryInt', aggfunc='sum').reset_index()
        pieDataSorted = pieData.sort_values(by=['NB_VictoryInt'], ascending=False)
        pieDataSorted.insert(1, "Color",pieDataSorted['Player_Name'].apply(lambda x: meeplesAssociated[x]))
        color_list = pieDataSorted['Color']
        pie = groupedVictoryPlaysRows[['Player_Name', 'NB_VictoryInt']].pivot_table(index=['Player_Name'], values='NB_VictoryInt', aggfunc='sum').sort_values(by=['NB_VictoryInt'], ascending=False).plot(kind="pie", y='NB_VictoryInt', ylabel='', colors=color_list, autopct=lambda p: format(p, '.1f') if p > 5 else None, subplots=True, legend = False)
        fig = pie[0].get_figure()
        fig.savefig(os.path.join(config.images_path, str(gameId) + "-np-result.png"), transparent=True)
        plt.cla()
        plt.close(fig)
        template = template.replace('{{victoryPie}}'     , os.path.join(config.images_path, str(gameId) + "-np-result.png") or "")
    else:
        template = template.replace('{{victoryPie}}'     , os.path.join(config.images_template, "looser-result.png") or "")
    
    
    results = ""
    victoryPlaysRows = groupedVictoryPlaysDF.loc[groupedVictoryPlaysDF['Id_Game'] == gameId]
    for vicIndex, victoryPlaysRow in victoryPlaysRows.iterrows():
        playerName = str(victoryPlaysRow['Player_Name'])
        
        if (playerName in meeplesAssociated):
            meepleColor = meeplesAssociated[playerName]
        else:
            if (len(meeplesAvailable) >0):
                meepleColor = meeplesAvailable[0]
                meeplesAvailable.remove(meepleColor)
            else:
                meepleColor = "white"
            
            meeplesAssociated[playerName] = meepleColor
        #EndIf
        
        results = results + "<b>"+playerName+"</b>: "
        victory = victoryPlaysRow['NB_VictoryInt']
        groupedPlaysPerPlayerForSelectedGameDF = groupedPlaysPerGamePerPlayerDF.loc[groupedPlaysPerGamePerPlayerDF['Id_Game'] == gameId]
        nbPlaysRows = groupedPlaysPerPlayerForSelectedGameDF.loc[groupedPlaysPerPlayerForSelectedGameDF['Player_Name'] == playerName]
        players_nbPlays = "999"
        if (len(nbPlaysRows.index) >=1):
            players_nbPlays = str(nbPlaysRows['Quantity'].values[0])
        results = results + str(victory) + " / " + players_nbPlays + " - "
        for x in range(0, victory):
            results = results + "<img src=\"" + os.path.join(config.images_template, "meeple-" + str(meepleColor) + ".png") + "\" class=\"meeple\"> "
        results = results + "<br>"
    #EndFor
    
    template = template.replace('{{Results}}'         , results                            or "N/A")
    
    #Write to output.html
    with open(config.output_not_play, 'a', encoding="utf-8") as file:
        file.write(template)



#Write the trailer.
write_output_trailer(config.output_not_play)



endtime = datetime.now()
totaltime = endtime - starttime
logging.info(f'command: {sys.argv}')
logging.info(f'total time: {totaltime}')
