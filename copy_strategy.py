import time
import traceback

import requests
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
import sqlalchemy
import pandas as pd
import undetected_chromedriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from get_all_strategies_db import all_strategies_pos
import ctrader_profiles_live
import get_strategy_positions
from assets import assets_dict
from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor
import datetime
import calendar
from ctrader_profiles_data import profiles_dict
import googleapiclient
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from api_inf import appClientId, appClientSecret, accessToken, sheet_id

currentAccountId = None

hostType = "demo"

bot_token = "5788844610:AAH2Bt0VvjP-_oNL8Ao3YgNszoW6msf_bHU"
chat_id = '-630654819'
#appClientId = "4555_wq3rXCrFhy1TzyHBJVi6NB3ASsEFJebAF9lkGvZ9y8UUOzTdPI"
#appClientSecret = "PI1MRMjmP6qaTwhfrPuUxf5NyiEEUJCyNaMGDmAMDKiwrmY47p"

#accessToken = "edvAZfX8UotwpAzCbcVs8_k9Cs-f2vV8UbXfFMBS-ac"

profile_id = 0
symbol_id = 0
to_do = ""
vol = 0
hrefn = ''

client = Client(EndPoints.PROTOBUF_LIVE_HOST if hostType.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST,
                    EndPoints.PROTOBUF_PORT, TcpProtocol)

def accountAuthResponseCallback(result):
    print("\nAccount authenticated")
    request = ProtoOASymbolsListReq()
    request.ctidTraderAccountId = profile_id
    request.includeArchivedSymbols = False
    deferred = client.send(request)

def applicationAuthResponseCallback(result):
    print("\nApplication authenticated")
    request = ProtoOAAccountAuthReq()
    request.ctidTraderAccountId = profile_id
    request.accessToken = accessToken
    deferred = client.send(request)
    deferred.addCallbacks(accountAuthResponseCallback, onError)


def connected(client):  # Callback for client connection
    print("\nConnected")
    request = ProtoOAApplicationAuthReq()
    request.clientId = appClientId
    request.clientSecret = appClientSecret
    deferred = client.send(request)
    deferred.addErrback(onError)

def disconnected(client, reason):
    print("\nDisconnected: ", reason)

def onMessageReceived(client, message):  # Callback for receiving all messages
    if message.payloadType in [ProtoOASubscribeSpotsRes().payloadType, ProtoOAAccountLogoutRes().payloadType,
                                   ProtoHeartbeatEvent().payloadType]:
        return
    elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
        print("API Application authorized\n")
        print(
                "Please use setAccount command to set the authorized account before sending any other command, try help for more detail\n")
        print("To get account IDs use ProtoOAGetAccountListByAccessTokenReq command")
        if currentAccountId is not None:
            sendProtoOAAccountAuthReq()
            return
    elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
        protoOAAccountAuthRes = Protobuf.extract(message)
        print(f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized\n")
        print("This acccount will be used for all future requests\n")
        print("You can change the account by using setAccount command")
    else:
        print("Message received: \n", Protobuf.extract(message))
    global toTrade
    if message.payloadType == ProtoOAExecutionEvent().payloadType:
        #print(message.deal)
        global posIds
        global hrefn
        if toTrade:
            #print("Execution event received")
            executionEvent = Protobuf.extract(message)
            #print(executionEvent.position.positionId)
            #if executionEvent.order.orderStatus == ""
            global new_df_posIds
            if executionEvent.order.orderStatus == 3:
                msg_text = f"Стратегия {hrefn} (счёт {href_n})\nОшибка открытия\n"
                send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
                response = requests.get(send_text)
            elif executionEvent.order.orderStatus == 1:
                #print(executionEvent.order.orderStatus)
                #new_df_posIds = []

                global symbol
                global side
                global vol
                try:
                    posId = executionEvent.position.positionId
                    new_df_posIds.append(posId)

                except:
                    posId = 0
                    new_df_posIds.append(posId)

                #msg_text = f"Стратегия {hrefn} (счёт {href_n})\nОткрыта позиция\n"
                #msg_text += f'''{symbol} - {side} - {vol / 10000}'''
                #send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
                #response = requests.get(send_text)

                global db
            #else:
            #    msg_text = f"Стратегия {hrefn} (счёт {href_n})\nОшибка открытия\n"
            #    send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
            #    response = requests.get(send_text)

                #trades_to_write.append([trade[1]["Date"], trade[1]["Symbol"], trade[1]["Side"], trade[1]["Currency"]])

                #trade_arr = [[trade[1]["Date"], trade[1]["Symbol"], trade[1]["Side"], trade[1]["Currency"]]]

                #resp = service.spreadsheets().values().append(
                #    spreadsheetId=sheet_id,
                #    range=f'''{href_n.replace("https://www.forexfactory.com/", "")}!A1''',
                #    valueInputOption="RAW",
                #    body={'values': trade_arr}).execute()


            #reactor.stop()
            #reactor.callLater(1, callable=reactor.stop)
            #reactor.stop()
        # send a reconcile request to receive a reconcile response
    # if message.payloadType == ProtoOAReconcileRes().payloadType:
    #     print("Reconcile response received")
    #     reconcileRes = Protobuf.extract(message)
    #     for position in reconcileRes.position:
    #         print(position.positionId)
    #     for order in reconcileRes.order:
    #         print(order.orderId)

    reactor.callLater(3, callable=executeUserCommand)


def onError(failure):  # Call back for errors
    print("Message Error: ", failure)

def setAccount(accountId):
    global currentAccountId
    if currentAccountId is not None:
        sendProtoOAAccountLogoutReq()
    currentAccountId = int(accountId)
    sendProtoOAAccountAuthReq()

def sendProtoOAVersionReq(clientMsgId=None):
    request = ProtoOAVersionReq()
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAGetAccountListByAccessTokenReq(clientMsgId=None):
    request = ProtoOAGetAccountListByAccessTokenReq()
    request.accessToken = accessToken
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAAccountLogoutReq(clientMsgId=None):
    request = ProtoOAAccountLogoutReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAAccountAuthReq(clientMsgId=None):
    request = ProtoOAAccountAuthReq()
    request.ctidTraderAccountId = currentAccountId
    request.accessToken = accessToken
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAAssetListReq(clientMsgId=None):
    request = ProtoOAAssetListReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAAssetClassListReq(clientMsgId=None):
    request = ProtoOAAssetClassListReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOASymbolCategoryListReq(clientMsgId=None):
    request = ProtoOASymbolCategoryListReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOASymbolsListReq(includeArchivedSymbols=False, clientMsgId=None):
    request = ProtoOASymbolsListReq()
    request.ctidTraderAccountId = currentAccountId
    request.includeArchivedSymbols = includeArchivedSymbols if type(includeArchivedSymbols) is bool else bool(
        includeArchivedSymbols)
    deferred = client.send(request)
    deferred.addErrback(onError)

def sendProtoOATraderReq(clientMsgId=None):
    request = ProtoOATraderReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAUnsubscribeSpotsReq(symbolId, clientMsgId=None):
    request = ProtoOAUnsubscribeSpotsReq()
    request.ctidTraderAccountId = currentAccountId
    request.symbolId.append(int(symbolId))
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOASubscribeSpotsReq(symbolId, timeInSeconds, subscribeToSpotTimestamp=False, clientMsgId=None):
    request = ProtoOASubscribeSpotsReq()
    request.ctidTraderAccountId = currentAccountId
    request.symbolId.append(int(symbolId))
    request.subscribeToSpotTimestamp = subscribeToSpotTimestamp if type(subscribeToSpotTimestamp) is bool else bool(
            subscribeToSpotTimestamp)
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)
    reactor.callLater(int(timeInSeconds), sendProtoOAUnsubscribeSpotsReq, symbolId)

def sendProtoOAReconcileReq(clientMsgId=None):
    request = ProtoOAReconcileReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAGetTrendbarsReq(weeks, period, symbolId, clientMsgId=None):
    request = ProtoOAGetTrendbarsReq()
    request.ctidTraderAccountId = currentAccountId
    request.period = ProtoOATrendbarPeriod.Value(period)
    request.fromTimestamp = int(
            calendar.timegm((datetime.datetime.utcnow() - datetime.timedelta(weeks=int(weeks))).utctimetuple())) * 1000
    request.toTimestamp = int(calendar.timegm(datetime.datetime.utcnow().utctimetuple())) * 1000
    request.symbolId = int(symbolId)
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAGetTickDataReq(days, quoteType, symbolId, clientMsgId=None):
    request = ProtoOAGetTickDataReq()
    request.ctidTraderAccountId = currentAccountId
    request.type = ProtoOAQuoteType.Value(quoteType.upper())
    request.fromTimestamp = int(
            calendar.timegm((datetime.datetime.utcnow() - datetime.timedelta(days=int(days))).utctimetuple())) * 1000
    request.toTimestamp = int(calendar.timegm(datetime.datetime.utcnow().utctimetuple())) * 1000
    request.symbolId = int(symbolId)
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOANewOrderReq(symbolId, orderType, tradeSide, volume, price=None, clientMsgId=None):
    request = ProtoOANewOrderReq()
    request.ctidTraderAccountId = currentAccountId
    request.symbolId = int(symbolId)
    request.orderType = ProtoOAOrderType.Value(orderType.upper())
    request.tradeSide = ProtoOATradeSide.Value(tradeSide.upper())
    request.volume = int(volume) * 100
    if request.orderType == ProtoOAOrderType.LIMIT:
        request.limitPrice = float(price)
    elif request.orderType == ProtoOAOrderType.STOP:
        request.stopPrice = float(price)
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendNewMarketOrder(symbolId, tradeSide, volume, clientMsgId=None):
    sendProtoOANewOrderReq(symbolId, "MARKET", tradeSide, volume, clientMsgId=clientMsgId)

def sendNewLimitOrder(symbolId, tradeSide, volume, price, clientMsgId=None):
    sendProtoOANewOrderReq(symbolId, "LIMIT", tradeSide, volume, price, clientMsgId)

def sendNewStopOrder(symbolId, tradeSide, volume, price, clientMsgId=None):
    sendProtoOANewOrderReq(symbolId, "STOP", tradeSide, volume, price, clientMsgId)

def sendProtoOAClosePositionReq(positionId, volume, clientMsgId=None):
    request = ProtoOAClosePositionReq()
    request.ctidTraderAccountId = currentAccountId
    request.positionId = int(positionId)
    request.volume = int(volume) * 100
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOACancelOrderReq(orderId, clientMsgId=None):
    request = ProtoOACancelOrderReq()
    request.ctidTraderAccountId = currentAccountId
    request.orderId = int(orderId)
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def executeUserCommand():
    global toTrade
    #print(toTrade)
    global profile_id
    #print(profile_id)
    global toClose
    global posId
    if toTrade:
        tradeExecute(profile_id)
        #setAccount(profile_id)
        #reactor.callLater(1, callable=orderExecute)
        #reactor.callLater(1, callable=reactor.stop)
    else:
        setAccount(profile_id)
        #sendProtoOAClosePositionReq(positionId=posId, volume=vol)
        reactor.callLater(1, callable=close_positions)
        reactor.callLater(len(toClose)+1, callable=reactor.stop)
    #reactor.stop()

def close_positions():
    global toClose
    global hrefn
    for pos in toClose:
        sendProtoOAClosePositionReq(positionId=pos[0], volume=pos[1])
        msg_text = f"Стратегия {hrefn} (счёт {href_n})\nЗакрыта позиция\n"
        msg_text += f'''{pos[2]} - {pos[3]} - {pos[1]}'''
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
        response = requests.get(send_text)

#toTrade = False

def save_db():
    #print(new_df_dates)
    #print(new_df_symbols)
    #print(new_df_sides)
    #print(new_df_currencies)
    #print(new_df_durations)
    #print(new_df_posIds)
    new_df = pd.DataFrame(
        {"Date": new_df_dates, "Symbol": new_df_symbols, "Side": new_df_sides, "Position Id": new_df_posIds, "On Site ID": on_site_ids})


    print(new_df)

    new_df.to_sql(f'{href_n}', db,
                  if_exists='append')

def orderExecute():
    global hrefn
    global symbol
    global side
    global vol
    global symbols_to_trade, to_dos, vols
    for i in range(len(symbols_to_trade)):
        print(symbols_to_trade[i], to_dos[i], vols[i])
        for s, id in assets_dict.items():
            if id == symbols_to_trade[i]:
                symbol = s
        #symbol = symbols_to_trade[i]
        side = to_dos[i]
        vol = vols[i]
        sendNewMarketOrder(symbols_to_trade[i], to_dos[i], vols[i])
        #time.sleep(1)
        msg_text = f"Стратегия {hrefn} (счёт {href_n})\nОткрыта позиция\n"
        msg_text += f'''{symbols_to_trade[i]} - {to_dos[i]} - {vols[i]/10000}'''
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
        response = requests.get(send_text)
    reactor.callLater(len(symbols_to_trade), callable=save_db)
    reactor.callLater(len(symbols_to_trade)+1, callable=reactor.stop)

def tradeExecute(id):
    setAccount(id)
    reactor.callLater(3, callable=orderExecute)

#sheet_id = "1qQ4rptKh48a0GYlWbTI1NnOo7lgcpU7x1Sm5qj8EXxM"
#sheet_id = "1xTtS5I8-1SG164Q27Sc60guEB1FLl3PavfHvrvHn0FE"

def get_all_open_trades(href, db_engine, id_p):
    #global profile_id
    global db
    db = db_engine

    #setAccount(ctrader_profiles_dict[href])

    #db_engine = db_engine

    symb = []
    sides = []
    dates = []
    currs = []
    durations = []
    global trades_to_write
    trades_to_write = []
    global href_n
    href_n = id_p
    global posIds

    global profile_id
    global hrefn
    hrefn = href

    profile_id = profiles_dict[href]["profileId"]

    quantity = profiles_dict[href]["quantity"] * 10


    #setAccount(profile_id)

    #print(profile_id)
    #global service
    #service = get_service_sacc()
    try:
        new_opened = get_strategy_positions.get_strategy_opened_positions(href)
    except Exception as ex:
        print(traceback.format_exc())
        send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text=PARSER ERROR!!!'
        response = requests.get(send_text)

    #print(trades)
    global traded
    traded = False

    try:
        global toTrade
        #print(href_n)
        #already_written = pd.read_sql(f'{href_n.replace("https://www.forexfactory.com/", "")}', db)

        global new_df_dates, new_df_symbols, new_df_sides
        new_df_dates = []
        new_df_symbols = []
        new_df_sides = []
        posIds = []
        global to_do
        global symbol_id
        global vol
        global posId
        global toClose
        global on_site_ids

        global symbols_to_trade
        global vols
        global to_dos

        global new_df_posIds
        new_df_posIds = []

        symbols_to_trade = []
        vols = []
        to_dos = []
        on_site_ids = []
        #print("XCX")

        #already_written = pd.read_sql("SELECT * FROM " + f'{href_n}', db)
        already_written = pd.read_sql("SELECT * FROM " +  f'''"{href_n}"''', db)

        for trade in new_opened.iterrows():
            not_in_flag = True
            ins_df = already_written.isin(
                {"On Site ID": [trade[1]["On Site ID"]]})
            for row in ins_df.iterrows():
                # print(row[1]["Date"], row[1]["Symbol"], row[1]["Side"], row[1]["Currency"])
                if row[1]["On Site ID"] == True:
                    not_in_flag = False
                #    print(not_in_flag)
            # if not_in_flag and "min" in trade[1]["Duration"]:
            if not_in_flag and datetime.datetime.timestamp(datetime.datetime.now()) - datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")) < 3600:
                new_df_dates.append(datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")))
                new_df_symbols.append(trade[1]["Symbol"])
                new_df_sides.append(trade[1]["Side"])
                to_do = trade[1]["Side"].upper()
                on_site_ids.append(trade[1]["On Site ID"])

                if trade[1]["Symbol"] in assets_dict:
                    symbol_id = assets_dict[trade[1]["Symbol"]]
                    if "XAU" in trade[1]["Symbol"] or "Us" in trade[1]["Symbol"]:
                        vol = 1 * quantity
                    elif "/" in trade[1]["Symbol"]:
                        vol = 1000 * quantity
                    else:
                        vol = 1000 * quantity

                    symbols_to_trade.append(symbol_id)
                    to_dos.append(to_do)
                    vols.append(vol)

                    toTrade = True
                    print(toTrade)

                    traded = True
                    #msg_text = f"Стратегия {href} (счёт {href_n})\nОткрыта позиция\n"
                    #msg_text += f'''{trade[1]["Symbol"]} - {trade[1]["Side"].upper()} - {quantity}'''
                    #send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
                    #response = requests.get(send_text)
        if len(symbols_to_trade) != 0:
            client.setConnectedCallback(connected)
            client.setDisconnectedCallback(disconnected)
            client.setMessageReceivedCallback(onMessageReceived)
            client.startService()
            reactor.run()
            return True

        new_df_dates = []
        new_df_symbols = []
        new_df_sides = []
        new_df_currencies = []
        new_df_durations = []
        posIds = []
        on_site_ids = []

        global toClose

        toClose = []
        #print("XCX")
        for trade in already_written.iterrows():
            in_flag = False
            ins_df = new_opened.isin({"On Site ID": [trade[1]["On Site ID"]]})
            for row in ins_df.iterrows():
                #print(row[1]["Date"], row[1]["Symbol"], row[1]["Side"], row[1]["Currency"])
                if row[1]["On Site ID"] == True:
                    in_flag = True
            if in_flag:
                new_df_dates.append(trade[1]["Date"])
                new_df_symbols.append(trade[1]["Symbol"])
                new_df_sides.append(trade[1]["Side"])
                posIds.append(trade[1]["Position Id"])
                on_site_ids.append(trade[1]["On Site ID"])
            else:
                if "XAU" in trade[1]["Symbol"] or "Us" in trade[1]["Symbol"]:
                    vol = 1 * quantity
                elif "/" in trade[1]["Symbol"]:
                    vol = 1000 * quantity
                else:
                    vol = 1 * quantity

                toClose.append([trade[1]["Position Id"], vol, trade[1]["Symbol"], trade[1]["Side"]])

                #msg_text = f"Стратегия {href} (счёт {href_n})\nЗакрыта позиция\n"
                #msg_text += f'''{trade[1]["Symbol"]} - {trade[1]["Side"].upper()} - {quantity}'''
                #send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
                #response = requests.get(send_text)

        new_df = pd.DataFrame({"Date": new_df_dates, "Symbol": new_df_symbols, "Side": new_df_sides, "Position Id": posIds, "On Site ID": on_site_ids})

        new_df.to_sql(f'{href_n}', db,
                      if_exists='replace')

        if len(toClose) != 0:

            toTrade = False

            #print(toTrade)

            posId = trade[1]["Position Id"]

            trades_to_write.append([trade[1]["Date"], trade[1]["Symbol"], trade[1]["Side"]])

            trade_arr = [[trade[1]["Date"], trade[1]["Symbol"], trade[1]["Side"]]]

            #resp = service.spreadsheets().values().append(
            #            spreadsheetId=sheet_id,
            #            range=f'''{href_n.replace("https://www.forexfactory.com/", "")}!A1''',
            #            valueInputOption="RAW",
            #            body={'values': trade_arr}).execute()


            traded = True

            client.setConnectedCallback(connected)
            client.setDisconnectedCallback(disconnected)
            client.setMessageReceivedCallback(onMessageReceived)
            #setAccount(profile_id)
            client.startService()
            reactor.run()
            return True

        # resp = service.spreadsheets().values().append(
        #     spreadsheetId=sheet_id,
        #     range=f'''{href_n.replace("https://www.forexfactory.com/", "")}!A1''',
        #     valueInputOption="RAW",
        #     body={'values': trades_to_write}).execute()
    except Exception as ex:
        #print(ex)
        #print(traceback.format_exc())
        already_written = pd.DataFrame()

        new_df_dates = []
        new_df_symbols = []
        new_df_sides = []
        posIds = []
        on_site_ids = []

        #global symbols_to_trade
        #global vols
        #global to_dos

        symbols_to_trade = []
        vols = []
        to_dos = []

        new_df_posIds = []

        # global to_do
        # global symbol_id
        # global vol

        for trade in new_opened.iterrows():
            #if "min" in trade[1]["Duration"]:
            if datetime.datetime.timestamp(datetime.datetime.now()) - datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")) < 3600:
                new_df_dates.append(datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")))
                new_df_symbols.append(trade[1]["Symbol"])
                new_df_sides.append(trade[1]["Side"])
                on_site_ids.append(trade[1]["On Site ID"])
                to_do = ""
                if trade[1]["Side"] == "Sell":
                    to_do = "Sell"
                else:
                    to_do = "Buy"

                if trade[1]["Symbol"] in assets_dict:
                    symbol_id = assets_dict[trade[1]["Symbol"]]
                    if "XAU" in trade[1]["Symbol"]:
                        vol = 1
                    elif "/" in trade[1]["Symbol"]:
                        vol = 1000 * quantity
                    else:
                        vol = 1000 * quantity

                    symbols_to_trade.append(symbol_id)
                    to_dos.append(to_do)
                    vols.append(vol)

                    toTrade = True
                    print(toTrade)

                    traded = True

                    #msg_text = f"Стратегия {href} (счёт {href_n})\nОткрыта позиция\n"
                    #msg_text += f'''{trade[1]["Symbol"]} - {trade[1]["Side"].upper()} - {quantity}'''
                    #send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg_text}'
                    #response = requests.get(send_text)

        if len(symbols_to_trade) != 0:
            client.setConnectedCallback(connected)
            client.setDisconnectedCallback(disconnected)
            client.setMessageReceivedCallback(onMessageReceived)
            client.startService()
            reactor.run()
            return True


#print("XCXC")

def check_if_in(symb, side, db_engine, id):
    try:
        df = pd.read_sql("SELECT * FROM " +  f'''"{id}"''', db_engine)
    except:
        return False

    res = df.loc[(df['Symbol'] == symb) & (df['Side'] == side)]

    return not res.empty

def main():
        global traded
        traded = False
        #toTrade = ""



        #all_open_trades_db = sqlalchemy.create_engine(f'sqlite:///D:\\forex_copytrader\\all_pos.db')
        #trades_db = sqlalchemy.create_engine(f'sqlite:///D:\\forex_copytrader\\my_pos.db')
        all_open_trades_db = sqlalchemy.create_engine(f'sqlite:///all_pos.db')
        trades_db = sqlalchemy.create_engine(f'sqlite:///my_pos.db')
        #all_strategies_pos(all_open_trades_db)
        global db
        db = trades_db

        ids = []

        hrefs = []

        for href in profiles_dict.keys():
            ids.append(profiles_dict[href]["profileId"])
            hrefs.append(href)

        for i in range(len(ids)-1):
            id1 = ids[i]

            href = hrefs[i]

            symb = []
            sides = []
            dates = []
            currs = []
            durations = []
            global trades_to_write
            trades_to_write = []
            global href_n
            href_n = ids[i]
            global posIds

            global profile_id
            global hrefn
            hrefn = href

            profile_id = profiles_dict[href]["profileId"]

            for j in range(i, len(ids)-1):
                id2 = ids[j+1]
                try:
                    i_df = pd.read_sql("SELECT * FROM " +  f'''"{id1}"''', all_open_trades_db)
                except:
                    continue

                #for pos in i_df.iterrows():

                global toTrade

                global new_df_dates, new_df_symbols, new_df_sides
                new_df_dates = []
                new_df_symbols = []
                new_df_sides = []
                global to_do
                global symbol_id
                global vol
                global posId
                global toClose
                global on_site_ids

                global symbols_to_trade
                global vols
                global to_dos

                global new_df_posIds
                new_df_posIds = []

                symbols_to_trade = []
                vols = []
                to_dos = []
                on_site_ids = []
                quantity = profiles_dict[href]["quantity"] * 10

                try:
                    opened_df = pd.read_sql("SELECT * FROM " + f'''"{id1}"''', trades_db)

                    for trade in i_df.iterrows():
                        if check_if_in(trade[1]["Symbol"], trade[1]["Side"], all_open_trades_db, id2):
                            if not check_if_in(trade[1]["Symbol"], trade[1]["Side"], trades_db, id1):
                                if trade[1]["Symbol"] in assets_dict:
                                    symbol_id = assets_dict[trade[1]["Symbol"]]
                                    if "XAU" in trade[1]["Symbol"] or "Us" in trade[1]["Symbol"]:
                                        vol = 1 * quantity
                                    elif "/" in trade[1]["Symbol"]:
                                         vol = 1000 * quantity
                                    else:
                                        vol = 1000 * quantity

                                    symbols_to_trade.append(symbol_id)
                                    to_dos.append(to_do)
                                    vols.append(vol)

                                    toTrade = True
                                    print(toTrade)

                                    traded = True
                    if len(symbols_to_trade) != 0:
                        client.setConnectedCallback(connected)
                        client.setDisconnectedCallback(disconnected)
                        client.setMessageReceivedCallback(onMessageReceived)
                        client.startService()
                        reactor.run()

                    new_df_dates = []
                    new_df_symbols = []
                    new_df_sides = []
                    new_df_currencies = []
                    new_df_durations = []
                    posIds = []
                    on_site_ids = []

                    global toClose

                    toClose = []
                    # print("XCX")
                    for trade in opened_df.iterrows():
                        in_flag = False
                        ins_df = i_df.isin({"On Site ID": [trade[1]["On Site ID"]]})
                        for row in ins_df.iterrows():
                            # print(row[1]["Date"], row[1]["Symbol"], row[1]["Side"], row[1]["Currency"])
                            if row[1]["On Site ID"] == True:
                                in_flag = True
                        if in_flag:
                            new_df_dates.append(trade[1]["Date"])
                            new_df_symbols.append(trade[1]["Symbol"])
                            new_df_sides.append(trade[1]["Side"])
                            posIds.append(trade[1]["Position Id"])
                            on_site_ids.append(trade[1]["On Site ID"])
                        else:
                            if "XAU" in trade[1]["Symbol"] or "Us" in trade[1]["Symbol"]:
                                vol = 1 * quantity
                            elif "/" in trade[1]["Symbol"]:
                                vol = 1000 * quantity
                            else:
                                vol = 1 * quantity

                            toClose.append([trade[1]["Position Id"], vol, trade[1]["Symbol"], trade[1]["Side"]])

                    new_df = pd.DataFrame(
                        {"Date": new_df_dates, "Symbol": new_df_symbols, "Side": new_df_sides, "Position Id": posIds,
                         "On Site ID": on_site_ids})

                    new_df.to_sql(f'{id1}', trades_db,
                                  if_exists='replace')

                    if len(toClose) != 0:
                        toTrade = False

                        posId = trade[1]["Position Id"]

                        trades_to_write.append([trade[1]["Date"], trade[1]["Symbol"], trade[1]["Side"]])

                        trade_arr = [[trade[1]["Date"], trade[1]["Symbol"], trade[1]["Side"]]]
                        traded = True

                        client.setConnectedCallback(connected)
                        client.setDisconnectedCallback(disconnected)
                        client.setMessageReceivedCallback(onMessageReceived)
                        # setAccount(profile_id)
                        client.startService()
                        reactor.run()
                        return True
                except:
                    new_df_dates = []
                    new_df_symbols = []
                    new_df_sides = []
                    posIds = []
                    on_site_ids = []

                    symbols_to_trade = []
                    vols = []
                    to_dos = []

                    new_df_posIds = []

                    for trade in i_df.iterrows():
                        #print(trade[1]["Date"])
                        #print(datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")))
                        if check_if_in(trade[1]["Symbol"], trade[1]["Side"], all_open_trades_db, id2) and datetime.datetime.timestamp(datetime.datetime.now()) - datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")) < 3600:
                            new_df_dates.append(datetime.datetime.timestamp(datetime.datetime.strptime(trade[1]["Date"], "%Y-%m-%d %H:%M:%S.%f")))
                            new_df_symbols.append(trade[1]["Symbol"])
                            new_df_sides.append(trade[1]["Side"])
                            on_site_ids.append(trade[1]["On Site ID"])
                            to_do = ""
                            if trade[1]["Side"] == "Sell":
                                to_do = "Sell"
                            else:
                                to_do = "Buy"

                            if trade[1]["Symbol"] in assets_dict:
                                symbol_id = assets_dict[trade[1]["Symbol"]]
                                if "XAU" in trade[1]["Symbol"]:
                                    vol = 1
                                elif "/" in trade[1]["Symbol"]:
                                    vol = 1000 * quantity
                                else:
                                    vol = 1000 * quantity

                                symbols_to_trade.append(symbol_id)
                                to_dos.append(to_do)
                                vols.append(vol)

                                toTrade = True
                                print(toTrade)

                                traded = True

                    if len(symbols_to_trade) != 0:
                        client.setConnectedCallback(connected)
                        client.setDisconnectedCallback(disconnected)
                        client.setMessageReceivedCallback(onMessageReceived)
                        client.startService()
                        reactor.run()

if __name__ == "__main__":
    main()
