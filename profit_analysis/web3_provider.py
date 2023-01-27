import ast
import os

import web3

POKT_ENDPOINT_BASE_URL = "https://poly-mainnet.gateway.pokt.network/v1/lb/"


class Web3Provider:
    def __init__(self, ind):
        web3_rpc_pocket_endpoints = os.environ.get("POCKET_ENDPOINTS_LIST")
        web3_rpc_urls = ast.literal_eval(web3_rpc_pocket_endpoints)
        self.web3_rpc_urls = web3_rpc_urls
        self.ind = ind
        self.w3_provider = create_web3(web3_rpc_urls, ind)


W3 = Web3Provider(0)


def create_web3(web3_rpc_urls, ind=0):
    web3_rpc_pocket_endpoint = web3_rpc_urls[ind]
    web3_rpc_url = POKT_ENDPOINT_BASE_URL + web3_rpc_pocket_endpoint
    w3_provider = web3.Web3(web3.Web3.HTTPProvider(web3_rpc_url))
    w3_provider.middleware_onion.inject(web3.middleware.geth_poa_middleware, layer=0)
    if w3_provider.isConnected():
        return w3_provider
    else:
        raise Exception("Failed to connect")
