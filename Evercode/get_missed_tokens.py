# импорт библиотек
import requests
import pandas as pd


def prepare_data(data, col_name, save_col=[], new_name=None):
    data = data[[col_name] + save_col].copy()
    data[col_name] = data[col_name].str.upper()
    if new_name:
        data.rename({col_name: new_name}, axis=1, inplace=True)
    return data

if __name__ == "__main__":
    # выгрузка данных по первому endpoint
    endpoint_1 = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start=1&limit=1500&sortBy=market_cap&sortType=desc&convert=USD,BTC,ETH&cryptoType=all&tagType=all&audited=false&aux=ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap"
    result_1 = requests.get(endpoint_1).json()
    # определение нужных колонок
    cc_colums = ["name", "volume24h", "marketCap", "cc_id", "cc_name", "cc_symbol", "cc_dateAdded"]
    # извлечение вложенных полей из quotes
    cclist_data = pd.json_normalize(result_1['data']['cryptoCurrencyList'], 'quotes',
                                    ["id", "name", "symbol", "dateAdded"], 'cc_')[cc_colums]
    # для определения volume24h берем данные по курсу в USD
    cclist_data = cclist_data[(cclist_data['name'] == 'USD')].reset_index(drop=True)
    """
    Cоединять источники есть смысл по полю symbol, так как так получается максимальная мощность пересечения.
    В первую очередь как будто очевидно соединить по полю id, но некоторые монеты в cc и ss имеют разные id,
    например, ABBC: cc_id = 3437, ss_id = 1094).
    По полю symbol в данных coinmarketcap есть дубли, для их устранения беру токены с максимальным временем добавления
    dateAdded и максимальной капитализацией marketCap. Если и в таком случае остаются дубли, беру первый случайный токен.
    """
    cclist_data = cclist_data.sort_values(["cc_symbol", "cc_dateAdded", "marketCap"],
                                          ascending=False).groupby("cc_symbol", as_index=False).first()
    cclist_data = cclist_data.drop_duplicates(subset='cc_symbol')
    assert cclist_data[cclist_data.duplicated(subset='cc_symbol', keep=False)].shape[0] == 0
    print(f"coinmarketcap data loaded, shape = {cclist_data.shape[0]}")

    # выгрузка данных по второму endpoint
    endpoint_2 = "https://simpleswap.io/api/v3/currencies?fixed=false&includeDisabled=false"
    result_2 = requests.get(endpoint_2).json()
    ss_data = pd.DataFrame(result_2)
    assert ss_data[ss_data.duplicated(subset='symbol', keep=False)].shape[0] == 0
    print(f"simpleswap.io data loaded, shape = {ss_data.shape[0]}")

    # подготовка данных к соединению, выделение уникальных токенов и монет из источников
    cc_tokens = prepare_data(cclist_data, "cc_symbol", ['cc_id', 'cc_name', "volume24h"])
    ss_tokens = prepare_data(ss_data, "symbol", ["id"], 'ss_symbol')

    # склейка данных
    total_tokens = cc_tokens.merge(ss_tokens, left_on='cc_symbol', right_on='ss_symbol', how="left")

    # монеты из эндпоинта coinmarketcap, которых нет в списке доступных у обменника https://simpleswap.io.
    missed_tokens = total_tokens[total_tokens['ss_symbol'].isna()][['cc_id', 'cc_name',
                                                                    'cc_symbol', 'volume24h']]
    # сортировка по убыванию volume24h
    missed_tokens.sort_values('volume24h', ascending=False, inplace=True)
    missed_tokens.reset_index(drop=True, inplace=True)
    print(f"missed tokens processed, shape = {missed_tokens.shape[0]}")

    # выгрузка в базу
    missed_tokens.to_csv("missed_tokens.csv", index=False)
    print(f"data 'missed_tokens.csv' saved to db")