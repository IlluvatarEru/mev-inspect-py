import ast
import os

import web3
from web3.eth import AsyncEth

from mev_inspect.provider import get_base_provider

POKT_ENDPOINT_BASE_URL = "https://poly-mainnet.gateway.pokt.network/v1/lb/"


class Web3Provider:
    def __init__(self):
        web3_rpc_pocket_endpoints = os.environ.get("POCKET_ENDPOINTS_LIST")
        if web3_rpc_pocket_endpoints is None:
            web3_rpc_urls = [os.environ.get("RPC_URL")]
        else:
            web3_rpc_urls = ast.literal_eval(web3_rpc_pocket_endpoints)
        self.web3_rpc_urls = web3_rpc_urls
        self.ind = 0
        self.current_rpc = self.web3_rpc_urls[0]
        self.w3_provider = create_web3(self.current_rpc)
        self.w3_provider_async = create_web3_async(self.current_rpc)

    def rotate_rpc_url(self):
        new_ind = (self.ind + 1) % len(self.web3_rpc_urls)
        self.ind = new_ind
        self.current_rpc = self.web3_rpc_urls[new_ind]
        self.w3_provider = create_web3(self.current_rpc)
        self.w3_provider_async = create_web3_async(self.current_rpc)


def create_web3_async(web3_rpc_pocket_endpoint, request_timeout=300):
    web3_rpc_url = POKT_ENDPOINT_BASE_URL + web3_rpc_pocket_endpoint
    base_provider = get_base_provider(web3_rpc_url, request_timeout=request_timeout)
    w3_base_provider = web3.Web3(
        base_provider, modules={"eth": (AsyncEth,)}, middlewares=[]
    )
    return w3_base_provider


def create_web3(web3_rpc_pocket_endpoint):
    web3_rpc_url = POKT_ENDPOINT_BASE_URL + web3_rpc_pocket_endpoint
    w3_provider = web3.Web3(web3.Web3.HTTPProvider(web3_rpc_url))
    w3_provider.middleware_onion.inject(web3.middleware.geth_poa_middleware, layer=0)
    if w3_provider.isConnected():
        return w3_provider
    else:
        raise Exception("Failed to connect")


W3 = Web3Provider()
