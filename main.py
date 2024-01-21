from web3 import Web3
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from eth_abi import abi
from abi_data import gmx_abi, staked_gmx_tracker_abi, gmx_vester_abi, multicall_abi
import logging
import time
import argparse

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
MULTICALL_ARBITRUM = "0xcA11bde05977b3631167028862bE2a173976CA11"
GMX_ARBITRUM_DEPLOYMENT_BLOCK = 147903

GMX_AVALANCHE = "0x62edc0692BD897D2295872a9FFCac5425011c661"
ESGMX_AVALANCHE = "0xFf1489227BbAAC61a9209A08929E4c2a526DdD17"
STAKED_GMX_TRACKER_AVALANCHE = "0x2bD10f8E93B3669b6d42E74eEedC65dd1B0a1342"
FEE_GMX_TRACKER_AVALANCHE = "0x4d268a7d4C16ceB5a606c173Bd974984343fea13"
BONUS_GMX_TRACKER_AVALANCHE = "0x908C4D94D34924765f1eDc22A1DD098397c59dD4"
GLP_AVALANCHE = "0x9e295B5B976a184B14aD8cd72413aD846C299660"
SGMX_AVALANCHE = "0x4d268a7d4C16ceB5a606c173Bd974984343fea13"
SGLP_AVALANCHE = "0xaE64d55a6f09E4263421737397D1fdFA71896a69"
GMX_VESTER_AVALANCHE = "0x472361d3cA5F49c8E633FB50385BfaD1e018b445"
GLP_VESTER_AVALANCHE = "0x62331A7Bd1dfB3A7642B7db50B5509E57CA3154A"
MULTICALL_AVALANCHE = "0xcA11bde05977b3631167028862bE2a173976CA11"
GMX_AVALANCHE_DEPLOYMENT_BLOCK = 8352150


TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BLOCK_RANGE_LIMIT = 9999
NUM_PROCESSES = 4
RPC_ARBITRUM = "https://arb-mainnet.g.alchemy.com/v2/gv37D3QuLk_vT2N2opLgMt7I7MM24aRO"
RPC_AVALANCHE = "https://avalanche-mainnet.infura.io/v3/0f3da4bda8514421b1952d7129074aca"


def choose_network():
    # print("Select a network:")
    # print("1: Arbitrum")
    # print("2: Avalanche")
    # while True:
    #     choice = input("Enter your choice (1 for Arbitrum, 2 for Avalanche): ")
    #     if choice == "1":
    #         return (
    #             "arbitrum",
    #             RPC_ARBITRUM,
    #             [GMX_ARBITRUM, GLP_ARBITRUM, SGMX_ARBITRUM, SGLP_ARBITRUM],
    #             [STAKED_GMX_TRACKER_ARBITRUM,ESGMX_ARBITRUM,FEE_GMX_TRACKER_ARBITRUM,BONUS_GMX_TRACKER_ARBITRUM,GMX_VESTER_ARBITRUM,GLP_VESTER_ARBITRUM,MULTICALL_ARBITRUM],
    #             GMX_ARBITRUM_DEPLOYMENT_BLOCK,
    #         )
    #     elif choice == "2":
    #         return (
    #             "avalanche",
    #             RPC_AVALANCHE,
    #             [GMX_AVALANCHE, GLP_AVALANCHE, SGMX_AVALANCHE, SGLP_AVALANCHE],
    #             [STAKED_GMX_TRACKER_AVALANCHE,ESGMX_AVALANCHE,FEE_GMX_TRACKER_AVALANCHE,BONUS_GMX_TRACKER_AVALANCHE,GMX_VESTER_AVALANCHE,GLP_VESTER_AVALANCHE,MULTICALL_AVALANCHE],
    #             GMX_AVALANCHE_DEPLOYMENT_BLOCK,
    #         )
    #     else:
    #         print("Invalid choice. Please enter 1 or 2.")
    return (
        "arbitrum",
        RPC_ARBITRUM,
        [GMX_ARBITRUM, GLP_ARBITRUM, SGMX_ARBITRUM, SGLP_ARBITRUM],
        [STAKED_GMX_TRACKER_ARBITRUM, ESGMX_ARBITRUM, FEE_GMX_TRACKER_ARBITRUM, BONUS_GMX_TRACKER_ARBITRUM, GMX_VESTER_ARBITRUM, GLP_VESTER_ARBITRUM, MULTICALL_ARBITRUM],
        GMX_ARBITRUM_DEPLOYMENT_BLOCK,
    )


def initialize_web3_connection(rpc_url):
    return Web3(Web3.HTTPProvider(rpc_url))


def fetch_account_data(account, gmx, staked_gmx_tracker, esgmx, glp, staked_fee_gmx_tracker, bonus_gmx_tracker, gmx_vester, glp_vester, contract_addresses, helper_contracts, multicall, max_retries=10, initial_wait=1):
    try:
        calls = [
            {"target": contract_addresses[0], "callData": gmx.encodeABI(fn_name="balanceOf", args=[account])},
            {"target": helper_contracts[0], "callData": staked_gmx_tracker.encodeABI(fn_name="depositBalances", args=[account, contract_addresses[0]])},
            {"target": helper_contracts[1], "callData": esgmx.encodeABI(fn_name="balanceOf", args=[account])},
            {"target": helper_contracts[0], "callData": staked_gmx_tracker.encodeABI(fn_name="depositBalances", args=[account, helper_contracts[1]])},
            {"target": helper_contracts[3], "callData": bonus_gmx_tracker.encodeABI(fn_name="claimable", args=[account])},
            {"target": helper_contracts[2], "callData": staked_fee_gmx_tracker.encodeABI(fn_name="stakedAmounts", args=[account])},
            {"target": contract_addresses[3], "callData": glp.encodeABI(fn_name="balanceOf", args=[account])},
            {"target": helper_contracts[4], "callData": gmx_vester.encodeABI(fn_name="getMaxVestableAmount", args=[account])},
            {"target": helper_contracts[5], "callData": glp_vester.encodeABI(fn_name="getMaxVestableAmount", args=[account])},
        ]

        # First multicall with retry mechanism
        result = None
        retry_count = 0
        while retry_count < max_retries:
            try:
                result = multicall.functions.aggregate(calls).call()
                break
            except Exception as e:
                logging.error(f"Error in first multicall for account {account}: {e}")
                retry_count += 1
                time.sleep(initial_wait * 2**retry_count)

        if retry_count == max_retries:
            logging.error(f"Failed to aggregate calls after {max_retries} attempts for account {account}")
            return None

        # Decoding results from the first multicall
        gmx_wallet = int(result[1][0].hex(), 16) / 10**18
        gmx_staked = int(result[1][1].hex(), 16) / 10**18
        esgmx_wallet = int(result[1][2].hex(), 16) / 10**18
        esgmx_staked = int(result[1][3].hex(), 16) / 10**18
        mp_wallet = int(result[1][4].hex(), 16) / 10**18
        mp_staked = int(result[1][5].hex(), 16) / 10**18
        glp_wallet = int(result[1][6].hex(), 16) / 10**18
        esgmx1 = int(result[1][7].hex(), 16)
        esgmx2 = int(result[1][8].hex(), 16)

        # Setup for the second set of multicall
        calls1 = [
            {"target": helper_contracts[4], "callData": gmx_vester.encodeABI(fn_name="getPairAmount", args=[account, esgmx1])},
            {"target": helper_contracts[5], "callData": glp_vester.encodeABI(fn_name="getPairAmount", args=[account, esgmx2])},
        ]

        # Second multicall with retry mechanism
        result1 = None
        retry_count = 0
        while retry_count < max_retries:
            try:
                result1 = multicall.functions.aggregate(calls1).call()
                break
            except Exception as e:
                logging.error(f"Error in second multicall for account {account}: {e}")
                retry_count += 1
                time.sleep(initial_wait * 2**retry_count)

        if retry_count == max_retries:
            logging.error(f"Failed to aggregate calls1 after {max_retries} attempts for account {account}")
            return None

        # Decoding results from the second multicall
        gmx_to_vest = int(result1[1][0].hex(), 16) / 10**18
        glp_to_vest = int(result1[1][1].hex(), 16) / 10**18

        # Constructing the data dictionary
        data = {
            "account": account,
            "GMX in wallet": gmx_wallet,
            "GMX staked": gmx_staked,
            "esGMX in wallet": esgmx_wallet,
            "esGMX staked": esgmx_staked,
            "MP in wallet": mp_wallet,
            "MP staked": mp_staked - (gmx_staked + esgmx_staked),
            "GLP in wallet": glp_wallet,
            "GLP staked": glp_wallet,
            "esGMX earned from GMX/esGMX/MPs": esgmx1 / 10**18,
            "GMX needed to vest": gmx_to_vest,
            "esGMX earned from GLP": esgmx2 / 10**18,
            "GLP needed to vest": glp_to_vest,
        }
        return data

    except Exception as e:
        logging.error(f"Error fetching data for account {account}: {e}")
        return None


if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(description='Process some integers.')

    # Add arguments
    parser.add_argument('-s', '--start', type=int, required=True, help='Start index')
    parser.add_argument('-e', '--end', type=int, required=True, help='End index')

    args = parser.parse_args()
    
    network_name, rpc_url, contract_addresses, helper_contracts, deployment_block = choose_network()

    web3 = initialize_web3_connection(rpc_url)

    gmx = web3.eth.contract(address=contract_addresses[0], abi=gmx_abi)
    staked_gmx_tracker = web3.eth.contract(address=helper_contracts[0], abi=staked_gmx_tracker_abi)
    esgmx = web3.eth.contract(address=helper_contracts[1], abi=gmx_abi)
    glp = web3.eth.contract(address=contract_addresses[3], abi=gmx_abi)
    staked_fee_gmx_tracker = web3.eth.contract(address=helper_contracts[2], abi=staked_gmx_tracker_abi)
    bonus_gmx_tracker = web3.eth.contract(address=helper_contracts[3], abi=staked_gmx_tracker_abi)
    gmx_vester = web3.eth.contract(address=helper_contracts[4], abi=gmx_vester_abi)
    glp_vester = web3.eth.contract(address=helper_contracts[5], abi=gmx_vester_abi)
    multicall = web3.eth.contract(address=helper_contracts[6], abi=multicall_abi)

    START_INDEX = args.start
    END_INDEX = args.end

    file_path = 'gmx_accounts_arbitrum.csv'
    df = pd.read_csv(file_path)
    unique_to_addresses = df["account"].iloc[START_INDEX:END_INDEX+1]


    with ThreadPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        # Submitting tasks to the executor
        future_to_account = {executor.submit(fetch_account_data, account, gmx, staked_gmx_tracker, esgmx, glp, staked_fee_gmx_tracker, bonus_gmx_tracker, gmx_vester, glp_vester, contract_addresses, helper_contracts, multicall): account for account in unique_to_addresses}

        # Using tqdm to display progress
        for future in tqdm(as_completed(future_to_account), total=len(unique_to_addresses), desc="Fetching accounts data"):
            account = future_to_account[future]
            try:
                account_data = future.result()
                if account_data:
                    # Find the index for the current account and update the DataFrame
                    index = df[df["account"] == account].index[0]
                    for key, value in account_data.items():
                        df.at[index, key] = value
            except Exception as e:
                print(f"Error processing data for account {account}: {str(e)}")

    df_subset = df.iloc[START_INDEX:END_INDEX+1]

    df_subset.to_csv(f"gmx_accounts_{network_name}_{START_INDEX}_{END_INDEX}.csv", index=False)
    print(f"CSV file created: gmx_accounts_{network_name}_{START_INDEX}_{END_INDEX}.csv")
