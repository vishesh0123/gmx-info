from web3 import Web3
import pandas as pd
from multiprocessing import Pool
from tqdm.contrib.concurrent import process_map
import itertools
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor , as_completed
from abi_data import gmx_abi , staked_gmx_tracker_abi , gmx_vester_abi

GMX_ARBITRUM = "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a"
ESGMX_ARBITRUM = "0xf42Ae1D54fd613C9bb14810b0588FaAa09a426cA"
STAKED_GMX_TRACKER_ARBITRUM = "0x908C4D94D34924765f1eDc22A1DD098397c59dD4"
FEE_GMX_TRACKER_ARBITRUM = "0xd2D1162512F927a7e282Ef43a362659E4F2a728F"
BONUS_GMX_TRACKER_ARBITRUM = "0x4d268a7d4C16ceB5a606c173Bd974984343fea13"
GLP_ARBITRUM = "0x4277f8F2c384827B5273592FF7CeBd9f2C1ac258"
SGMX_ARBITRUM = "0x908C4D94D34924765f1eDc22A1DD098397c59dD4"
SGLP_ARBITRUM = "0x1aDDD80E6039594eE970E5872D247bf0414C8903"
GMX_VESTER_ARBITRUM = "0x199070DDfd1CFb69173aa2F7e20906F26B363004"
GLP_VESTER_ARBITRUM = "0xA75287d2f8b217273E7FCD7E86eF07D33972042E"
GMX_ARBITRUM_DEPLOYMENT_BLOCK = 147903

GMX_AVALANCHE = "0x62edc0692BD897D2295872a9FFCac5425011c661"
ESGMX_AVALANCHE = "0xFf1489227BbAAC61a9209A08929E4c2a526DdD17"
STAKED_GMX_TRACKER_AVALANCHE="0x2bD10f8E93B3669b6d42E74eEedC65dd1B0a1342"
FEE_GMX_TRACKER_AVALANCHE='0x4d268a7d4C16ceB5a606c173Bd974984343fea13'
BONUS_GMX_TRACKER_AVALANCHE="0x908C4D94D34924765f1eDc22A1DD098397c59dD4"
GLP_AVALANCHE = "0x9e295B5B976a184B14aD8cd72413aD846C299660"
SGMX_AVALANCHE = "0x4d268a7d4C16ceB5a606c173Bd974984343fea13"
SGLP_AVALANCHE = "0xaE64d55a6f09E4263421737397D1fdFA71896a69"
GMX_VESTER_AVALANCHE="0x472361d3cA5F49c8E633FB50385BfaD1e018b445"
GLP_VESTER_AVALANCHE="0x62331A7Bd1dfB3A7642B7db50B5509E57CA3154A"
GMX_AVALANCHE_DEPLOYMENT_BLOCK = 8352150


TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BLOCK_RANGE_LIMIT = 9999
NUM_PROCESSES = 4
RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/GCYsGX1wP9QOpItP7s4o5mqfCbnLHWjr"
RPC_AVALANCHE="https://1rpc.io/5Fg5wv6Qf93EVMjYn/avax/c"

def choose_network():
    print("Select a network:")
    print("1: Arbitrum")
    print("2: Avalanche")
    while True:
        choice = input("Enter your choice (1 for Arbitrum, 2 for Avalanche): ")
        if choice == "1":
            return (
                "arbitrum",
                RPC_ARBITRUM,
                [GMX_ARBITRUM, GLP_ARBITRUM, SGMX_ARBITRUM, SGLP_ARBITRUM],
                [STAKED_GMX_TRACKER_ARBITRUM,ESGMX_ARBITRUM,FEE_GMX_TRACKER_ARBITRUM,BONUS_GMX_TRACKER_ARBITRUM,GMX_VESTER_ARBITRUM,GLP_VESTER_ARBITRUM],
                GMX_ARBITRUM_DEPLOYMENT_BLOCK,
            )
        elif choice == "2":
            return (
                "avalanche",
                RPC_AVALANCHE,
                [GMX_AVALANCHE, GLP_AVALANCHE, SGMX_AVALANCHE, SGLP_AVALANCHE],
                [STAKED_GMX_TRACKER_AVALANCHE,ESGMX_AVALANCHE,FEE_GMX_TRACKER_AVALANCHE,BONUS_GMX_TRACKER_AVALANCHE,GMX_VESTER_AVALANCHE,GLP_VESTER_AVALANCHE],
                GMX_AVALANCHE_DEPLOYMENT_BLOCK,
            )
        else:
            print("Invalid choice. Please enter 1 or 2.")

def initialize_web3_connection(rpc_url):
    return Web3(Web3.HTTPProvider(rpc_url))

def create_log_filter_params(contract_address, start_block, end_block, event_signature):
    return {
        "fromBlock": start_block,
        "toBlock": end_block,
        "address": contract_address,
        "topics": [event_signature],
    }

def fetch_logs_for_range(args):
    range_info, rpc_url, contract_addresses = args
    web3 = initialize_web3_connection(rpc_url)
    start_block, end_block = range_info
    all_logs = []
    for address in contract_addresses:
        filter_params = create_log_filter_params(address, start_block, end_block, TRANSFER_EVENT_SIGNATURE)
        logs = web3.eth.get_logs(filter_params)
        all_logs.extend(logs)
    return all_logs

def divide_into_chunks(start_block, end_block, chunk_size):
    ranges = []
    while start_block < end_block:
        batch_end_block = min(start_block + chunk_size, end_block)
        ranges.append((start_block, batch_end_block))
        start_block = batch_end_block + 1
    return ranges

def is_eoa_parallel(web3, address):
    try:
        code = web3.eth.get_code(address)
        return address if (code == b'') else None
    except Exception as e:
        print(f"Error checking address {address}: {str(e)}")
        return None

def extract_to_addresses(logs):
    to_addresses = set()
    for log in logs:
        encoded_address = log["topics"][2]
        decoded_address = Web3.to_checksum_address(encoded_address.hex()[-40:])
        to_addresses.add(decoded_address)
    return list(to_addresses)

def fetch_account_data(account, gmx, staked_gmx_tracker, esgmx, glp, staked_fee_gmx_tracker, bonus_gmx_tracker, gmx_vester, glp_vester,contract_addresses,helper_contracts):
    try:
        gmx_staked =  staked_gmx_tracker.functions.depositBalances(account, contract_addresses[0]).call() / 10**18
        esgmx_staked = staked_gmx_tracker.functions.depositBalances(account, helper_contracts[1]).call() / 10**18
        glp_wallet = glp.functions.balanceOf(account).call() / 10**18
        esgmx1 = gmx_vester.functions.getMaxVestableAmount(account).call()
        esgmx2 = glp_vester.functions.getMaxVestableAmount(account).call()
        data = {
            'account': account,
            'GMX in wallet': gmx.functions.balanceOf(account).call() / 10**18,
            'GMX staked':gmx_staked,
            'esGMX in wallet': esgmx.functions.balanceOf(account).call() / 10**18,
            'esGMX staked': esgmx_staked,
            'MP in wallet': bonus_gmx_tracker.functions.claimable(account).call() / 10**18,
            'MP staked': (staked_fee_gmx_tracker.functions.stakedAmounts(account).call() / 10**18) - (gmx_staked + esgmx_staked),
            'GLP in wallet': glp_wallet,
            'GLP staked': glp_wallet,
            'esGMX earned from GMX/esGMX/MPs':esgmx1 / 10**18,
            'GMX needed to vest': gmx_vester.functions.getPairAmount(account,esgmx1).call() / 10**18,
            'esGMX earned from GLP': esgmx2 / 10**18,
            'GLP needed to vest': glp_vester.functions.getPairAmount(account, esgmx2).call() / 10**18
        }
        return data
    except Exception as e:
        print(f"Error fetching data for account {account}: {str(e)}")
        return None


if __name__ == "__main__":
    network_name,rpc_url, contract_addresses,helper_contracts,deployment_block = choose_network()
    latest_block_number = initialize_web3_connection(rpc_url).eth.block_number
    block_ranges = divide_into_chunks(deployment_block, latest_block_number, BLOCK_RANGE_LIMIT)
    args = [(range_info, rpc_url, contract_addresses) for range_info in block_ranges]
    chunksize = 20

    all_logs = process_map(fetch_logs_for_range, args, chunksize=chunksize, max_workers=NUM_PROCESSES)
    all_logs_flat = list(itertools.chain(*all_logs))
    unique_to_addresses = extract_to_addresses(all_logs_flat)

    web3 = initialize_web3_connection(rpc_url)
    
    gmx = web3.eth.contract(address=contract_addresses[0], abi=gmx_abi)
    staked_gmx_tracker = web3.eth.contract(address=helper_contracts[0],abi=staked_gmx_tracker_abi)
    esgmx = web3.eth.contract(address=helper_contracts[1],abi=gmx_abi)
    glp = web3.eth.contract(address=contract_addresses[3],abi=gmx_abi)
    staked_fee_gmx_tracker = web3.eth.contract(address=helper_contracts[2],abi=staked_gmx_tracker_abi)
    bonus_gmx_tracker = web3.eth.contract(address=helper_contracts[3],abi=staked_gmx_tracker_abi)
    gmx_vester = web3.eth.contract(address=helper_contracts[4],abi=gmx_vester_abi)
    glp_vester = web3.eth.contract(address=helper_contracts[5],abi=gmx_vester_abi)

    print(f'Found {len(unique_to_addresses)} Unique addresses')

    with ThreadPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        # Create a list to store futures
        futures = [executor.submit(is_eoa_parallel, web3, address) for address in unique_to_addresses]
        
        # Process the futures as they complete
        eoa_addresses = []
        for future in tqdm(as_completed(futures), total=len(unique_to_addresses), desc="Filtering out contract addresses", unit="addr"):
            result = future.result()
            if result:
                eoa_addresses.append(result)

    df = pd.DataFrame(columns=['account', 'GMX in wallet', 'GMX staked', 'esGMX in wallet', 'esGMX staked', 'GLP in wallet', 'GLP staked', 'MP in wallet', 'MP staked','esGMX earned from GMX/esGMX/MPs', 'GMX needed to vest', 'esGMX earned from GLP','GLP needed to vest'])

    df['account'] = eoa_addresses

    print(f'Found {len(eoa_addresses)} Unique account(EOA) addresses')

    # for index, account in tqdm(enumerate(eoa_addresses), total=len(eoa_addresses), desc="Fetching accounts data"):

    #     df.at[index,'GMX in wallet'] = gmx.functions.balanceOf(account).call() / 10**18
    #     df.at[index,'GMX staked'] = staked_gmx_tracker.functions.depositBalances(account,contract_addresses[0]).call() / 10**18
    #     df.at[index,'esGMX in wallet'] = esgmx.functions.balanceOf(account).call() / 10**18
    #     df.at[index,'esGMX staked'] = staked_gmx_tracker.functions.depositBalances(account,helper_contracts[1]).call() / 10**18
    #     df.at[index,'MP in wallet'] = bonus_gmx_tracker.functions.claimable(account).call() / 10**18
    #     df.at[index,'MP staked'] = (staked_fee_gmx_tracker.functions.stakedAmounts(account).call() / 10**18) - ( df.at[index,'GMX staked'] + df.at[index,'esGMX staked'])
    #     df.at[index, 'GLP in wallet'] = glp.functions.balanceOf(account).call() / 10**18
    #     df.at[index,'GLP staked'] = df.at[index, 'GLP in wallet']
    #     esgmx1 = gmx_vester.functions.getMaxVestableAmount(account).call()
    #     df.at[index,'esGMX earned from GMX/esGMX/MPs'] = esgmx1 / 10**18
    #     df.at[index,'GMX needed to vest'] = gmx_vester.functions.getPairAmount(account,esgmx1).call() / 10**18
    #     esgmx2 = glp_vester.functions.getMaxVestableAmount(account).call()
    #     df.at[index,'esGMX earned from GLP'] = esgmx2 / 10**18
    #     df.at[index,'GLP needed to vest'] = glp_vester.functions.getPairAmount(account, esgmx2).call() / 10**18

    with ThreadPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        # Submitting tasks to the executor
        future_to_account = {executor.submit(fetch_account_data, account, gmx, staked_gmx_tracker, esgmx, glp, staked_fee_gmx_tracker, bonus_gmx_tracker, gmx_vester, glp_vester,contract_addresses,helper_contracts): account for account in eoa_addresses}

        # Using tqdm to display progress
        for future in tqdm(as_completed(future_to_account), total=len(eoa_addresses), desc="Fetching accounts data"):
            account = future_to_account[future]
            try:
                account_data = future.result()
                if account_data:
                    # Find the index for the current account and update the DataFrame
                    index = df[df['account'] == account].index[0]
                    for key, value in account_data.items():
                        df.at[index, key] = value
            except Exception as e:
                print(f"Error processing data for account {account}: {str(e)}")

    df.to_csv(f'gmx_accounts_{network_name}_{latest_block_number}.csv', index=False)
    print('CSV file created: gmx_accounts.csv')

