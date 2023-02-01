import ast
import os

import web3
from web3.eth import AsyncEth

from mev_inspect.provider import get_base_provider


class Web3Provider:
    def __init__(self):
        web3_rpc_pocket_endpoints = os.environ.get("RPC_ENDPOINTS_LIST")
        print(f"Loading the following endpoints: {web3_rpc_pocket_endpoints}")
        rpc_url = os.environ.get("RPC_URL")
        rpc_endpoint_base_url, rpc_endpoint_key = split_rpc_url(rpc_url)
        self.rpc_endpoint_base_url = rpc_endpoint_base_url + "/"
        # default to the RPC URL in memory
        if web3_rpc_pocket_endpoints is None:
            web3_rpc_urls = [rpc_endpoint_key]
        else:
            web3_rpc_urls = ast.literal_eval(web3_rpc_pocket_endpoints)
        self.web3_rpc_urls = web3_rpc_urls
        self.ind = 0
        self.current_rpc = self.web3_rpc_urls[0]
        self.w3_provider = create_web3(self.rpc_endpoint_base_url, self.current_rpc)
        self.w3_provider_async = create_web3_async(
            self.rpc_endpoint_base_url, self.current_rpc
        )
        self.w3_provider_archival_eth = create_web3_archival_ethereum()

    def rotate_rpc_url(self):
        new_ind = (self.ind + 1) % len(self.web3_rpc_urls)
        self.ind = new_ind
        self.current_rpc = self.web3_rpc_urls[new_ind]
        self.w3_provider = create_web3(self.rpc_endpoint_base_url, self.current_rpc)
        self.w3_provider_async = create_web3_async(
            self.rpc_endpoint_base_url, self.current_rpc
        )
        self.w3_provider_archival_eth = create_web3_archival_ethereum(self.current_rpc)


def split_rpc_url(rpc_url):
    """
    Given a RPC url, we extract the key part and return the base url and the key
    """
    if rpc_url[-1] == "/":
        rpc_url = rpc_url[:-1]
    rpc_endpoint_base_url = "/".join(rpc_url.split("/")[:-1])
    rpc_endpoint_key = rpc_url.split("/")[-1]
    return rpc_endpoint_base_url, rpc_endpoint_key


def create_web3_async(base_url, web3_rpc_pocket_endpoint, request_timeout=300):
    web3_rpc_url = base_url + web3_rpc_pocket_endpoint
    print(f"Setting RPC={web3_rpc_url}")
    base_provider = get_base_provider(web3_rpc_url, request_timeout=request_timeout)
    w3_base_provider = web3.Web3(
        base_provider, modules={"eth": (AsyncEth,)}, middlewares=[]
    )
    return w3_base_provider


def create_web3(base_url, web3_rpc_pocket_endpoint):
    web3_rpc_url = base_url + web3_rpc_pocket_endpoint
    print(f"Connecting to RPC: {web3_rpc_url}")
    w3_provider = web3.Web3(web3.Web3.HTTPProvider(web3_rpc_url))
    w3_provider.middleware_onion.inject(web3.middleware.geth_poa_middleware, layer=0)
    return w3_provider


def create_web3_archival_ethereum(pokt_endpoint_key=None):
    if pokt_endpoint_key is None:
        rpc_url = os.environ.get("RPC_URL")
        _, pokt_endpoint_key = split_rpc_url(rpc_url)
    web3_rpc_url = (
        "https://eth-archival.gateway.pokt.network/v1/lb/" + pokt_endpoint_key
    )
    w3_provider = web3.Web3(web3.Web3.HTTPProvider(web3_rpc_url))
    w3_provider.middleware_onion.inject(web3.middleware.geth_poa_middleware, layer=0)
    return w3_provider


W3 = Web3Provider()
