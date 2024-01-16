from web3 import Web3
import pandas as pd
from multiprocessing import Pool
from tqdm.contrib.concurrent import process_map  # For progress bar with multiprocessing
import itertools

TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
#arbitrum constants
GMX_ARBITRUM = "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a"
GLP_ARBITRUM = "0x1aDDD80E6039594eE970E5872D247bf0414C8903"
SGMX_ARBITRUM = "0x908C4D94D34924765f1eDc22A1DD098397c59dD4"
SGLP_ARBITRUM = "0x1aDDD80E6039594eE970E5872D247bf0414C8903"
GMX_ARBITRUM_DEPLOYMENT_BLOCK = 147903

#avalanche constants
GMX_AVALANCHE = "0x62edc0692BD897D2295872a9FFCac5425011c661"
GLP_AVALANCHE = "0x9e295B5B976a184B14aD8cd72413aD846C299660"
SGMX_AVALANCHE = "0x4d268a7d4C16ceB5a606c173Bd974984343fea13"
SGLP_AVALANCHE = "0xaE64d55a6f09E4263421737397D1fdFA71896a69"
GMX_AVALANCHE_DEPLOYMENT_BLOCK = 8352150


BLOCK_RANGE_LIMIT = 9999  # Maximum range for each query
NUM_PROCESSES = 4  # Number of processes to use

# Function to show network menu and get user's choice
def choose_network():
    print("Select a network:")
    print("1: Arbitrum")
    print("2: Avalanche")
    
    while True:
        choice = input("Enter your choice (1 for Arbitrum, 2 for Avalanche): ")
        if choice == '1':
            return "https://1rpc.io/arb", "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a", 147903
        elif choice == '2':
            return "https://1rpc.io/avax/c", "0x62edc0692BD897D2295872a9FFCac5425011c661", 8352150
        else:
            print("Invalid choice. Please enter 1 or 2.")

# Initialize Web3 connection
def initialize_web3_connection(rpc_url):
    return Web3(Web3.HTTPProvider(rpc_url))

# Create parameters for the log filter
def create_log_filter_params(contract_address, start_block, end_block, event_signature):
    return {
        'fromBlock': start_block,
        'toBlock': end_block,
        'address': contract_address,
        'topics': [event_signature]
    }

def fetch_logs_for_range(range_info, rpc_url, contract_address):
    web3 = initialize_web3_connection(rpc_url)
    start_block, end_block = range_info
    filter_params = create_log_filter_params(contract_address, start_block, end_block, TRANSFER_EVENT_SIGNATURE)
    return web3.eth.get_logs(filter_params)

# Divide total blocks into chunks
def divide_into_chunks(start_block, end_block, chunk_size):
    ranges = []
    while start_block < end_block:
        batch_end_block = min(start_block + chunk_size, end_block)
        ranges.append((start_block, batch_end_block))
        start_block = batch_end_block + 1
    return ranges

def helper_fetch_logs(args):
    return fetch_logs_for_range(*args)

if __name__ == "__main__":
    rpc_url, contract_address, deployment_block = choose_network()
    # latest_block_number = initialize_web3_connection(rpc_url).eth.block_number
    latest_block_number = 157903
    block_ranges = divide_into_chunks(deployment_block, latest_block_number, BLOCK_RANGE_LIMIT)

    args = [(range_info, rpc_url, contract_address) for range_info in block_ranges]
    chunksize = 20  # Adjust this value as needed

    # Using a helper function instead of lambda
    all_logs = process_map(helper_fetch_logs, args, chunksize=chunksize, max_workers=NUM_PROCESSES)

    # Flatten list of logs
    all_logs_flat = list(itertools.chain(*all_logs))

    # Output the fetched logs
    print(all_logs_flat)