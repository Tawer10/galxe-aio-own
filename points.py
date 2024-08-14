from fake_useragent import UserAgent
from loguru import logger
import aiohttp
import asyncio
import random
import csv

GALXE_CAMPAIGN = 'B3'
SHUFFLE = False
BATCH_SIZE = 5
RETRY = 3

# Load wallets
with open('files/addresses.txt', "r", encoding='utf-8') as f:
    wallets = [row.strip() for row in f]

# wallets = wallets[:101]  # Limiting the list to 101 wallets

# Load and process proxies
with open('files/proxies.txt', "r", encoding='utf-8') as f:
    proxies = f.read().splitlines()
    # proxies = [f"{p.split(':')[-2]}:{p.split(':')[-1]}@{p.split(':')[0]}:{p.split(':')[1]}" for p in proxies]
    proxies = [p if '://' in p.split('|')[0] or p == '' else 'http://' + p for p in proxies]

ids = [i for i in range(1, len(wallets) + 1)]

def shuffle_lists_in_unison(list1, list2, list3):
    combined = list(zip(list1, list2, list3))
    random.shuffle(combined)
    return zip(*combined)

async def check_wallet(session, i, address, proxy, alias, attempt=0):
    try:
        url = 'https://graphigo.prd.galaxy.eco/query'
        headers = {
            'authority': 'graphigo.prd.galaxy.eco',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://galxe.com',
            'user-agent': str(UserAgent().random),
        }
        # proxy_config = f"http://{proxy}"

        json_data = {
            'operationName': 'SpaceAccessQuery',
            'variables': {
                'alias': alias,
                'address': address.lower(),
            },
            'query': 'query SpaceAccessQuery($id: Int, $alias: String, $address: String!) {\n  space(id: $id, alias: $alias) {\n    id\n    isFollowing\n    discordGuildID\n    discordGuildInfo\n    status\n    isAdmin(address: $address)\n    unclaimedBackfillLoyaltyPoints(address: $address)\n    addressLoyaltyPoints(address: $address) {\n      id\n      points\n      rank\n      __typename\n    }\n    __typename\n  }\n}\n',
        }

        async with session.post(url=url, json=json_data, headers=headers, proxy=proxy) as response:
            data = await response.json()
            points = data['data']['space']['addressLoyaltyPoints']['points']
            rank = data['data']['space']['addressLoyaltyPoints']['rank']
            logger.success(f'{i}) {address} {points} points : {rank} rank')

            return points, rank

    except Exception as e:
        if attempt < RETRY:
            logger.warning(f'{i}) {address} trying again, attempt {attempt + 1}: {e}')
            return await check_wallet(session, i, address, proxy, alias, attempt=attempt + 1)
        else:
            logger.error(f'{i}) {address} failed after {RETRY} attempts: {e}')
            return 0, 0

async def process_batch(session, ids_batch, wallet_batch, proxy_batch):
    tasks = []
    for i, wallet, proxy in zip(ids_batch, wallet_batch, proxy_batch):
        tasks.append(asyncio.ensure_future(check_wallet(session, i, wallet, proxy, GALXE_CAMPAIGN)))

    results = await asyncio.gather(*tasks)

    for wallet, (points, rank) in zip(wallet_batch, results):
        with open(f'results/points_{GALXE_CAMPAIGN.lower()}.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                wallet, rank, points
            ])

async def main():
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(wallets), BATCH_SIZE):
            await process_batch(session, ids[i:i + BATCH_SIZE], wallets[i:i + BATCH_SIZE], proxies[i:i + BATCH_SIZE])
            await asyncio.sleep(random.uniform(1, 3))  # Use uniform to avoid integer-only sleep

if __name__ == "__main__":
    with open(f'results/points_{GALXE_CAMPAIGN.lower()}.csv', 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            'Wallets', 'Rank', 'Points'
        ])

    # print(len(wallets), len((proxies)))
    if SHUFFLE:
        wallets, proxies, ids = shuffle_lists_in_unison(wallets, proxies, ids)
    asyncio.run(main())
