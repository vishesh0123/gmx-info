from web3 import Web3
import pandas as pd
from multiprocessing import Pool
from tqdm.contrib.concurrent import process_map
import itertools
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor , as_completed
from eth_abi import abi
from abi_data import gmx_abi , staked_gmx_tracker_abi , gmx_vester_abi , multicall_abi

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
MULTICALL_ARBITRUM= "0xcA11bde05977b3631167028862bE2a173976CA11"
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
MULTICALL_AVALANCHE="0xcA11bde05977b3631167028862bE2a173976CA11"
GMX_AVALANCHE_DEPLOYMENT_BLOCK = 8352150


TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BLOCK_RANGE_LIMIT = 9999
NUM_PROCESSES = 4
RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/GCYsGX1wP9QOpItP7s4o5mqfCbnLHWjr"
RPC_AVALANCHE="https://avalanche-mainnet.infura.io/v3/2228b0132c8e468ca39f6b8744215656"

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
                [STAKED_GMX_TRACKER_ARBITRUM,ESGMX_ARBITRUM,FEE_GMX_TRACKER_ARBITRUM,BONUS_GMX_TRACKER_ARBITRUM,GMX_VESTER_ARBITRUM,GLP_VESTER_ARBITRUM,MULTICALL_ARBITRUM],
                GMX_ARBITRUM_DEPLOYMENT_BLOCK,
            )
        elif choice == "2":
            return (
                "avalanche",
                RPC_AVALANCHE,
                [GMX_AVALANCHE, GLP_AVALANCHE, SGMX_AVALANCHE, SGLP_AVALANCHE],
                [STAKED_GMX_TRACKER_AVALANCHE,ESGMX_AVALANCHE,FEE_GMX_TRACKER_AVALANCHE,BONUS_GMX_TRACKER_AVALANCHE,GMX_VESTER_AVALANCHE,GLP_VESTER_AVALANCHE,MULTICALL_AVALANCHE],
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

def extract_to_addresses(logs):
    to_addresses = set()
    for log in logs:
        encoded_address = log["topics"][2]
        decoded_address = Web3.to_checksum_address(encoded_address.hex()[-40:])
        to_addresses.add(decoded_address)
    return list(to_addresses)


def fetch_account_data(account, gmx, staked_gmx_tracker, esgmx, glp, staked_fee_gmx_tracker, bonus_gmx_tracker, gmx_vester, glp_vester,contract_addresses,helper_contracts,multicall):
    try:
        
        calls =[]
        gmx_wallet = gmx.encodeABI(fn_name='balanceOf',args=[account])
        gmx_staked = staked_gmx_tracker.encodeABI(fn_name='depositBalances',args=[account,contract_addresses[0]])
        esgmx_wallet = esgmx.encodeABI(fn_name='balanceOf',args=[account])
        esgmx_staked = staked_gmx_tracker.encodeABI(fn_name='depositBalances',args=[account,helper_contracts[1]])
        mp_wallet = bonus_gmx_tracker.encodeABI(fn_name='claimable',args=[account])
        mp_staked = staked_fee_gmx_tracker.encodeABI(fn_name='stakedAmounts',args=[account])
        glp_wallet = glp.encodeABI(fn_name='balanceOf',args=[account])
        esgmx1 = gmx_vester.encodeABI(fn_name='getMaxVestableAmount',args=[account])
        esgmx2 = glp_vester.encodeABI(fn_name='getMaxVestableAmount',args=[account])
        
        calls.append({"target":contract_addresses[0],"callData":gmx_wallet})
        calls.append({"target":helper_contracts[0],"callData":gmx_staked})
        calls.append({"target":helper_contracts[1],"callData":esgmx_wallet})
        calls.append({"target":helper_contracts[0],"callData":esgmx_staked})
        calls.append({"target":helper_contracts[3],"callData":mp_wallet})
        calls.append({"target":helper_contracts[2],"callData":mp_staked})
        calls.append({"target":contract_addresses[3],"callData":glp_wallet})
        calls.append({"target":helper_contracts[4],"callData":esgmx1})
        calls.append({"target":helper_contracts[5],"callData":esgmx2})
        
        
        result = multicall.functions.aggregate(calls).call()
        result = result[1]

        gmx_wallet = (abi.decode(['uint256'],result[0]))[0] / 10**18
        gmx_staked = (abi.decode(['uint256'],result[1]))[0] / 10**18
        esgmx_wallet = (abi.decode(['uint256'],result[2]))[0] / 10**18
        esgmx_staked = (abi.decode(['uint256'],result[3]))[0] / 10**18
        mp_wallet = (abi.decode(['uint256'],result[4]))[0] / 10**18
        mp_staked = (abi.decode(['uint256'],result[5]))[0] / 10**18
        glp_wallet = (abi.decode(['uint256'],result[6]))[0] / 10**18
        esgmx1 = (abi.decode(['uint256'],result[7]))[0]
        esgmx2 = (abi.decode(['uint256'],result[8]))[0]

        gmx_to_vest = gmx_vester.encodeABI(fn_name='getPairAmount',args=[account,esgmx1])
        glp_to_vest = glp_vester.encodeABI(fn_name='getPairAmount',args=[account,esgmx2])

        calls1=[]

        calls1.append({"target":helper_contracts[4],"callData":gmx_to_vest})
        calls1.append({"target":helper_contracts[5],"callData":glp_to_vest})

        result = multicall.functions.aggregate(calls1).call()
        result = result[1]

        gmx_to_vest = (abi.decode(['uint256'],result[0]))[0] / 10**18
        glp_to_vest = (abi.decode(['uint256'],result[1]))[0] / 10**18



        
        data = {
            'account': account,
            'GMX in wallet': gmx_wallet,
            'GMX staked':gmx_staked,
            'esGMX in wallet': esgmx_wallet,
            'esGMX staked': esgmx_staked,
            'MP in wallet': mp_wallet,
            'MP staked': mp_staked - (gmx_staked + esgmx_staked),
            'GLP in wallet': glp_wallet,
            'GLP staked': glp_wallet,
            'esGMX earned from GMX/esGMX/MPs':esgmx1 / 10**18,
            'GMX needed to vest': gmx_to_vest,
            'esGMX earned from GLP': esgmx2 / 10**18,
            'GLP needed to vest': glp_to_vest
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
    multicall = web3.eth.contract(address=helper_contracts[6],abi=multicall_abi)

    print(f'Found {len(unique_to_addresses)} Unique addresses')

    df = pd.DataFrame(columns=['account', 'GMX in wallet', 'GMX staked', 'esGMX in wallet', 'esGMX staked', 'GLP in wallet', 'GLP staked', 'MP in wallet', 'MP staked','esGMX earned from GMX/esGMX/MPs', 'GMX needed to vest', 'esGMX earned from GLP','GLP needed to vest'])

    df['account'] = unique_to_addresses

    with ThreadPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        # Submitting tasks to the executor
        future_to_account = {executor.submit(fetch_account_data, account, gmx, staked_gmx_tracker, esgmx, glp, staked_fee_gmx_tracker, bonus_gmx_tracker, gmx_vester, glp_vester,contract_addresses,helper_contracts,multicall): account for account in unique_to_addresses}

        # Using tqdm to display progress
        for future in tqdm(as_completed(future_to_account), total=len(unique_to_addresses), desc="Fetching accounts data"):
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
    print(f'CSV file created: gmx_accounts_{network_name}_{latest_block_number}.csv')

