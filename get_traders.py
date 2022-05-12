import requests
import pymongo
from datetime import datetime
from tqdm import tqdm
import schedule

mongo_db_client = pymongo.MongoClient("mongodb://fransafu:password123@localhost:27017/")
traders_db = mongo_db_client.traders
traders_info_collection = traders_db.info

base_api_url = "https://www.bitget.com/v1/trigger"

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
}

cookies = {
    "afUserId": "f5e948b6-fa7f-49ec-a9c9-7c09ad687f04-p",
    "bt_rtoken": "upex:session:id:d04773ae048436026807fbba23ad6ed0ad46db248127fc2ef684aab0b903d956",
    "bt_sessonid": "CZACCOUNT_LOGIN_811962574907bac30a3d2bc8b5029e3f_E1914BDD7DAE47A7AF2992ACF821A910",
    "bt_newsessionid": "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIwZjEwMTUxYS04MjFjLTQ5YzEtODYxYi1hNjU4MDQyOTM3YzMxMDY4NjI3MTk3IiwidWlkIjoieHFrNzZRWjZmdUhXaGVtME1rWSs0dz09Iiwic3ViIjoiZnJhbnNhZnVAZ21haWwuY29tIiwiaXAiOiJuQlRFTWFqVHppUnlSaVFoSVRlZnB3PT0iLCJkaWQiOiJIL016S3pSTlJ2RzhHSU9iSGdZNVN1RUFjQm94MDBraWNGRU9LeC9YeWlwMTJSSXNaUUtNNUhCa1M0eVRSaG5XIiwic3RzIjowLCJpYXQiOjE2NTIyMzM1NTcsImV4cCI6MTY2MDAwOTU1NywicHVzaGlkIjoiZGRrU0xHVUNqT1J3WkV1TWswWVoxZz09IiwiaXNzIjoidXBleCJ9._Pr6kzKyBgRXfv9-BWaE3Pcf2tk04lAKDwHSRn2liGY"
}

def get_traders(page_number, page_size = 9):
    traders_url = f'{base_api_url}/trace/public/traderView'

    payload = {
        "model": 2,
        "sortRule": 0,
        "simulation": 0,
        "pageNo": page_number,
        "pageSize": page_size,
        "nickName": "",
        "fullStatus": 1,
        "languageType": 7
    }

    response = requests.post(traders_url, json=payload, headers=headers)
    if response.status_code != 200:
        return None
    return response.json()["data"]["rows"]

def get_stats(trader):
    stats = {}

    stats["nickname"] = trader["traderNickName"]
    stats["uid"] = trader["traderUid"]

    stats["is_full"] = trader["followCount"] == trader["maxFollowCount"]

    for vo in trader["itemVoList"]:
        stats[vo["showColumnDesc"]] = vo["showColumnValue"]

    stats["available_instruments"] = trader["openFollowProducts"]

    stats["profit"] = []
    for profit in trader["klineProfit"]["rows"]:
        stats["profit"].append({
            "amount": profit["amount"],
            "data_time": profit["dataTime"]
        })

    return stats

def get_trader_detail(traderUid):
    trader_detail = f'{base_api_url}/trace/trader/traderDetailPage'

    payload = {
        "languageType": 7,
        "traderUid": traderUid
    }

    response = requests.post(trader_detail, cookies=cookies, json=payload, headers=headers)
    if response.status_code != 200:
        return None

    return response.json()["data"]

def get_trader_followers(traderUid, page_number = 1):
    trader_followers = f'{base_api_url}/trace/trader/followerList'

    payload = {
        "languageType": 7,
        "pageNo": page_number,
        "pageSize": 10,
        "traderUid": traderUid
    }

    response = requests.post(trader_followers, cookies=cookies, json=payload, headers=headers)
    if response.status_code != 200:
        return None

    return response.json()["data"]["rows"]

# 
def operation_history_list(traderUid):
    trade_history_list = f'{base_api_url}/trace/order/historyList'

    payload = {
        "languageType": 7,
        "pageNo": 2,
        "pageSize": 10,
        "traderUid": traderUid
    }

    response = requests.post(trade_history_list, cookies=cookies, json=payload, headers=headers)
    if response.status_code != 200:
        return None

    return response.json()["data"]["rows"]

# Obtiene el volumen de transacciones realizadas en un rango de fecha
def trade_volumen(traderUid):
    trade_history_list = f'{base_api_url}/view/queryTradeVolume'

    payload = {
        "languageType": 7,
        "showDay": 1, # Last 7 Days
        "triggerUserId": traderUid
    }

    response = requests.post(trade_history_list, cookies=cookies, json=payload, headers=headers)
    if response.status_code != 200:
        return None

    return response.json()["data"]["rows"]


def trade_preference(traderUid):
    trade_preference = f'{base_api_url}/view/queryTradePreference'

    payload = {
        "languageType": 7,
        "triggerUserId": traderUid
    }

    response = requests.post(trade_preference, cookies=cookies, json=payload, headers=headers)
    if response.status_code != 200:
        return None

    return response.json()["data"]["rows"]

def trade_position(traderUid):
    '''
        Posiciones del trader en los ultimos 7 dias
    '''
    trade_position = f'{base_api_url}/view/queryPosition'

    payload = {
        "languageType": 7,
        "triggerUserId": traderUid
    }

    response = requests.post(trade_position, cookies=cookies, json=payload, headers=headers)
    if response.status_code != 200:
        return None

    return response.json()["data"]["rows"]

#trade_position_resp = trade_position("6045813505")
#for x in trade_position_resp:
#    print(x["positionTime"]) # positionTime / 1000 -> min
#    print(x["profit"]) 


def main():
    start_time = datetime.now()
    trader_page_size = 9
    page_number = 1
    print("getting traders...")
    traders = get_traders(page_number, trader_page_size)
    print("Total traders: " + str(len(traders)))

    while len(traders) > 0:
        print("The page of traders is the number " + str(page_number))

        print("for each traders... ")
        for trader in tqdm(traders, desc = 'for each traders'):
            trader_info = {}
            trader_info["created_at"] = datetime.today().replace(microsecond=0)
            trader_info["trader_uid"] = trader["traderUid"]
            traderUid = trader["traderUid"]
            print("The trader Uid is " + traderUid)

            print("Getting stats...")
            trader_info["stats"] = get_stats(trader)
            print("Getting stats... done")

            print("Getting trader detail...")
            trader_info["trader_detail"] = get_trader_detail(traderUid)
            print("Getting trader detail... done")

            print("Getting followers...")
            followers = []
            followers_page_number = 1
            while True:
                resp = get_trader_followers(traderUid, followers_page_number)
                if len(resp) == 0:
                    break
                followers += resp
                followers_page_number += 1
            trader_info["followers"] = followers
            print("Getting followers... done")

            print("Getting operation history...")
            trader_info["operation_history"] = operation_history_list(traderUid)
            print("Getting operation history... done")

            print("Getting trade volumen...")
            trader_info["trade_volumen"] = []
            for trade_vol in trade_volumen(traderUid):
                trader_info["trade_volumen"].append({
                    "trade_volumen": trade_vol["tradeVolume"],
                    "data_time": trade_vol["dataTime"]
                })
            print("Getting trade volumen... done")

            print("Getting trade preference...")
            trader_info["trade_preference"] = []
            for trade_pref in trade_preference(traderUid):
                trader_info["trade_preference"].append({
                    "preference_name": trade_pref["displayName"],
                    "amount": trade_pref["amount"]
                })
            print("Getting trade preference... done")

            print("Getting trade position...")
            trader_info["trade_position"] = []
            for trade_pos in trade_position(traderUid):
                trader_info["trade_position"].append({
                    "position_time": trade_pos["positionTime"],
                    "profit": trade_pos["profit"]
                })
            print("Getting trade position... done")
            # print(trader_info)
            traders_info_collection.insert_one(trader_info)
            # break

        page_number += 1
        traders = get_traders(page_number, trader_page_size)
        #r = traders_info_collection.find({})
        #list(map(print, r))
    print('Duration: {}'.format(datetime.now() - start_time))

if __name__ == "__main__":
    main()
    #schedule.every(3).hours.do(main)

    #while True:
    #     schedule.run_pending()
