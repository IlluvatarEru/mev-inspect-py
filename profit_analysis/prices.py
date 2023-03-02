import asyncio
import json
from time import sleep
from typing import Union

import pandas as pd
from profit_analysis.column_names import BLOCK_KEY, PRICE_KEY
from profit_analysis.constants import DATA_PATH

from mev_inspect.chains import ETHEREUM_CHAIN, POLYGON_CHAIN
from mev_inspect.web3_provider import W3

UNISWAP_V2_PAIR_ABI = json.loads(
    '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'
)
UNISWAP_V2_FACTORY_ABI = json.loads(
    '[{"inputs":[{"internalType":"address","name":"_feeToSetter","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"},{"constant":true,"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"allPairs","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"allPairsLength","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"}],"name":"createPair","outputs":[{"internalType":"address","name":"pair","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"feeTo","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"feeToSetter","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"getPair","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_feeTo","type":"address"}],"name":"setFeeTo","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_feeToSetter","type":"address"}],"name":"setFeeToSetter","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"}]'
)
UNISWAP_V3_FACTORY_ABI = '[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":true,"internalType":"int24","name":"tickSpacing","type":"int24"}],"name":"FeeAmountEnabled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnerChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":false,"internalType":"int24","name":"tickSpacing","type":"int24"},{"indexed":false,"internalType":"address","name":"pool","type":"address"}],"name":"PoolCreated","type":"event"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"name":"createPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"int24","name":"tickSpacing","type":"int24"}],"name":"enableFeeAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint24","name":"","type":"uint24"}],"name":"feeAmountTickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint24","name":"","type":"uint24"}],"name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"parameters","outputs":[{"internalType":"address","name":"factory","type":"address"},{"internalType":"address","name":"token0","type":"address"},{"internalType":"address","name":"token1","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"int24","name":"tickSpacing","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"}],"name":"setOwner","outputs":[],"stateMutability":"nonpayable","type":"function"}]'

ERC20_ABI = json.loads(
    '[ {"constant": true, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": false, "stateMutability": "view", "type": "function"}]'
)
UNISWAP_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
QUICKSWAP_FACTORY = "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32"
USDC_TOKEN_ADDRESS_ETHEREUM = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDC_TOKEN_ADDRESS_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
NULL_ADDRESS = "0x0000000000000000000000000000000000000000"
UNISWAP_V2_DEXES = ["uniswapv2", "quickswap"]
UNISWAP_V3_DEXES = ["uniswapv3"]


def determine_base_token(chain):
    chain = chain.lower()
    switcher = {
        ETHEREUM_CHAIN: USDC_TOKEN_ADDRESS_ETHEREUM,
        POLYGON_CHAIN: USDC_TOKEN_ADDRESS_POLYGON,
    }
    return switcher.get(chain, f"Invalid chain@ {chain}")


def read_factories(chain):
    factories = pd.read_csv(DATA_PATH + "factories.csv")
    factories = factories.loc[factories["chain"] == chain, "factory"].unique()
    return factories


class DEXPricer:
    def __init__(self, w3_provider, chain):
        self.w3_provider = w3_provider
        self._chain = chain
        self._factories = read_factories(chain)
        self._factory_index = 0
        self._factory_abi = self.set_factory_abi()
        self._pair = None
        self._token_base_address = determine_base_token(chain)
        self._token_target_address = None
        self._token_base_decimals = None
        self._token_target_decimals = None
        self._is_target_token0_or_token1 = None
        self._max_retries = None
        self.block_to_price = {}

    async def get_decimals_from_token(self, token):
        contract = self.w3_provider.w3_provider_archival.eth.contract(
            address=token, abi=ERC20_ABI
        )
        decimals = await contract.functions.decimals().call()
        return decimals

    def rotate_factory(self):
        # TODO: Add more DEXes so we can rotate
        self._factory_index = (self._factory_index + 1) % len(self._factories)
        self._factory = self._factories[self._factory_index]
        self.set_factory_abi()

    def set_factory_abi(self):
        if self._factory in UNISWAP_V2_DEXES:
            return UNISWAP_V2_FACTORY_ABI
        elif self._factory in UNISWAP_V3_DEXES:
            return UNISWAP_V3_FACTORY_ABI
        else:
            raise Exception(f"Factory {self._factory} does not have an associated ABI")

    async def create(self, token_target_address, max_retries=24):
        self._max_retries = max_retries
        if max_retries > 0:
            trials = 0
            n_trials = 3
            while trials < n_trials:
                trials += 1
                try:
                    self._token_target_address = token_target_address
                    if token_target_address != self._token_base_address:
                        factory = self.w3_provider.w3_provider_archival.eth.contract(
                            address=self._factory, abi=self._factory_abi
                        )
                        pair_address = await factory.functions.getPair(
                            self._token_base_address, token_target_address
                        ).call()
                        if pair_address == NULL_ADDRESS:
                            print(
                                f"Pair address is null for the pool of USDC vs {token_target_address}"
                            )
                            # if there is no pool for this pair on the given factory, try to rotate factories to see if
                            # another DEX has the pool
                            self.rotate_factory()
                            return await self.create(
                                token_target_address, max_retries - n_trials
                            )
                        pair_contract = (
                            self.w3_provider.w3_provider_archival.eth.contract(
                                address=pair_address, abi=UNISWAP_V2_PAIR_ABI
                            )
                        )
                        self._pair = pair_contract
                        self._token_base_decimals = (
                            10
                            ** await self.get_decimals_from_token(
                                self._token_base_address
                            )
                        )
                        self._token_target_decimals = (
                            10
                            ** await self.get_decimals_from_token(token_target_address)
                        )
                        target_token = await self.is_target_token0_or_token1()
                        self._is_target_token0_or_token1 = target_token
                    return self
                except Exception as e:
                    print(f"Error ({trials}/{n_trials}), retrying  create  -  {e}")
                    sleep(0.05)
            W3.rotate_rpc_url()
            return await self.create(token_target_address, max_retries - n_trials)
        else:
            return self

    async def is_target_token0_or_token1(self):
        if await self._pair.functions.token0().call() == self._token_target_address:
            return 0
        elif await self._pair.functions.token1().call() == self._token_target_address:
            return 1
        else:
            raise Exception(
                f"Target token ({self._token_target_address}) not in contract pair {self._pair}"
            )

    async def get_price_at_block(self, block_number: Union[int, float]):
        if self._factory in UNISWAP_V2_DEXES:
            return self.get_price_at_block_uniswapv2(block_number)
        elif self._factory in UNISWAP_V3_DEXES:
            return self.get_price_at_block_uniswapv3(block_number)
        else:
            raise Exception(f"Factory {self._factory} is not supported.")

    async def get_price_at_block_uniswapv3(self, block_number: Union[int, float]):
        return block_number

    async def get_price_at_block_uniswapv2(self, block_number: Union[int, float]):
        trials = 0
        n_trials = 3
        if self._max_retries > 0:
            while trials < n_trials:
                trials += 1
                try:
                    if self._token_target_address == self._token_base_address:
                        price = 1.0
                    else:
                        reserves = await self._pair.functions.getReserves().call(
                            block_identifier=int(block_number)
                        )
                        if self._is_target_token0_or_token1 == 0:
                            token_target_reserves = reserves[0]
                            token_base_reserves = reserves[1]
                        else:
                            token_target_reserves = reserves[1]
                            token_base_reserves = reserves[0]
                        price = (
                            (float(token_base_reserves) / float(token_target_reserves))
                            * self._token_target_decimals
                            / self._token_base_decimals
                        )

                    price = float(price)
                    self.block_to_price[block_number] = price
                    return price
                except Exception as e:
                    print(
                        f"Error ({trials}/{n_trials}), retrying get_price_at_block {block_number}  - {e}"
                    )
                    sleep(0.05)
            W3.rotate_rpc_url()
            await self.create(self._token_target_address, self._max_retries - n_trials)
            return await self.get_price_at_block(block_number)
        else:
            return 0.0


async def safe_get_price(pricer, block, max_concurrency_semaphore):
    async with max_concurrency_semaphore:
        return await pricer.get_price_at_block(block)


async def get_decimal(token_address, chain=POLYGON_CHAIN):
    print(f"Requesting decimals for {token_address}")
    pricer = DEXPricer(W3, chain)
    decimal = await pricer.get_decimals_from_token(token_address)
    return decimal


async def get_uniswap_historical_prices(
    target_blocks,
    token_address,
    chain=POLYGON_CHAIN,
    max_concurrency=10,
    block_batch_size=1024,
):
    """

    :return: pd.DataFrame, with columns = [BLOCK_KEY, PRICE_KEY]
    """
    target_blocks = [int(b) for b in target_blocks]
    print(f"Requesting prices for {token_address} for {len(target_blocks)} blocks")
    if len(target_blocks) > 1:
        pricer = DEXPricer(W3, chain)
        # we use USDC as a base token
        await pricer.create(token_address)
        tasks = []
        max_concurrency_semaphore = asyncio.Semaphore(max_concurrency)

        for start_block_number_index in range(0, len(target_blocks), block_batch_size):
            end_block_number_index = min(
                len(target_blocks) - 1, start_block_number_index + block_batch_size
            )
            for k in range(end_block_number_index - start_block_number_index):
                block = target_blocks[start_block_number_index + k]
                tasks.append(
                    asyncio.ensure_future(
                        safe_get_price(pricer, int(block), max_concurrency_semaphore)
                    )
                )
        await asyncio.gather(*tasks)
        block_to_price = pricer.block_to_price
        prices = pd.DataFrame(
            list(block_to_price.items()), columns=[BLOCK_KEY, PRICE_KEY]
        )
        if len(prices) > 0:
            prices = prices.loc[prices[PRICE_KEY] > 0]
        prices[BLOCK_KEY] = pd.to_numeric(prices[BLOCK_KEY], downcast="integer")
        prices = prices.sort_values(by=[BLOCK_KEY])
        return prices
    else:
        # get prices for 10 blocks on each side in case the nodes are out of sync for the target block
        target_block = int(target_blocks[0])
        n_blocks_on_each_side = 10
        target_blocks = (
            [target_block - i for i in range(n_blocks_on_each_side)]
            + [target_block]
            + [target_block + i for i in range(n_blocks_on_each_side)]
        )
        return await get_uniswap_historical_prices(
            target_blocks,
            token_address,
            chain,
            max_concurrency,
            block_batch_size,
        )
