#!/usr/bin/env python3
"""DEX Early Executability Feasibility V0.3a.1: prospective READ ONLY Base research.

Standard library only. READ ONLY on Base, no wallet, private key, signature,
eth_send*, transaction, trade, price prediction, or execution PnL.

Scopes two factory contracts: Uniswap V3 and Aerodrome classic. Discovers
prospective pool creation, watches raw Mint/Swap plus removal/initialization
or reserve-sync events, and optionally polls DexScreener and GeckoTerminal.
Requires a Base HTTPS RPC that permits eth_getLogs on small block ranges.
"""
from __future__ import annotations

import argparse
from decimal import Decimal, localcontext, InvalidOperation
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

VERSION = "0.3a.1"
CHAIN = 8453
SCHEMA = 4
UNI_FACTORY = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
AERO_FACTORY = "0x420dd381b31aef6683db6b902084cb0ffece40da"
DEFAULT_RPC = "https://base-mainnet.g.alchemy.com/public"
QUOTER_V2 = "0x3d4e44eb1374240ce5f1b871ab261cd16335b76a"
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ANCHORS = {'usdc': USDC, 'weth': WETH}
SIZES = {'usdc': ('10', '50'), 'weth': ('0.005', '0.02')}
BUY_DELAYS = (15, 60)
SELL_HORIZONS = (5, 15, 30)

DEX_URL = "https://api.dexscreener.com/latest/dex/pairs/base/"
GECKO_URL = "https://api.geckoterminal.com/api/v2/networks/base/pools/"
HEADERS = {"User-Agent": "DEX-Early-Onchain/0.3a.1 research read-only", "Accept": "application/json"}
GECKO_HEADERS = {**HEADERS, "Accept":"application/json;version=20230203"}

# Ethereum Keccak-256, not standardized SHA3-256. Embedded to avoid dependencies.
_ROUNDS = [0x0000000000000001,0x0000000000008082,0x800000000000808a,0x8000000080008000,
 0x000000000000808b,0x0000000080000001,0x8000000080008081,0x8000000000008009,
 0x000000000000008a,0x0000000000000088,0x0000000080008009,0x000000008000000a,
 0x000000008000808b,0x800000000000008b,0x8000000000008089,0x8000000000008003,
 0x8000000000008002,0x8000000000000080,0x000000000000800a,0x800000008000000a,
 0x8000000080008081,0x8000000000008080,0x0000000080000001,0x8000000080008008]
_ROT = [0,1,62,28,27,36,44,6,55,20,3,10,43,25,39,41,45,15,21,8,18,2,61,56,14]
_MASK=(1<<64)-1

def keccak256(raw:bytes)->bytes:
    def rol(x,n): return ((x<<n)|(x>>(64-n)))&_MASK if n else x
    def perm(a):
        for rc in _ROUNDS:
            c=[a[x]^a[x+5]^a[x+10]^a[x+15]^a[x+20] for x in range(5)]
            d=[c[(x-1)%5]^rol(c[(x+1)%5],1) for x in range(5)]
            for y in range(5):
                for x in range(5):a[x+5*y]^=d[x]
            b=[0]*25
            for y in range(5):
                for x in range(5):b[y+5*((2*x+3*y)%5)]=rol(a[x+5*y],_ROT[x+5*y])
            for y in range(5):
                for x in range(5):a[x+5*y]=b[x+5*y]^((~b[((x+1)%5)+5*y])&b[((x+2)%5)+5*y])
            a[0]^=rc
    buf=bytearray(raw);buf.append(1)
    while len(buf)%136!=135:buf.append(0)
    buf.append(128);state=[0]*25
    for off in range(0,len(buf),136):
        for i in range(17):state[i]^=int.from_bytes(buf[off+8*i:off+8*i+8],'little')
        perm(state)
    return b''.join(x.to_bytes(8,'little') for x in state)[:32]

def topic(sig:str)->str:return '0x'+keccak256(sig.encode()).hex()
FACTORY_TOPICS={
 'uniswap_v3':topic('PoolCreated(address,address,uint24,int24,address)'),
 'aerodrome_classic':topic('PoolCreated(address,address,bool,address,uint256)')}
EVT_SIG={
 'v3_initialize':'Initialize(uint160,int24)',
 'v3_mint':'Mint(address,address,int24,int24,uint128,uint256,uint256)',
 'v3_swap':'Swap(address,address,int256,int256,uint160,uint128,int24)',
 'v3_burn':'Burn(address,int24,int24,uint128,uint256,uint256)',
 'aero_mint':'Mint(address,uint256,uint256)',
 'aero_swap':'Swap(address,address,uint256,uint256,uint256,uint256)',
 'aero_burn':'Burn(address,address,uint256,uint256)',
 'aero_sync':'Sync(uint256,uint256)'}
EVT_TOPICS={k:topic(v) for k,v in EVT_SIG.items()}
TOPIC_EVENT={v:k for k,v in EVT_TOPICS.items()}
PROTOCOL_ALLOWED={
 'uniswap_v3':set(k for k in EVT_SIG if k.startswith('v3_')),
 'aerodrome_classic':set(k for k in EVT_SIG if k.startswith('aero_'))}
assert FACTORY_TOPICS['uniswap_v3']=='0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
assert EVT_TOPICS['v3_swap']=='0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'

class DataError(RuntimeError):pass
class TransportError(RuntimeError):
    def __init__(self, kind, status=None, request_id=None, diagnostic=None):
        super().__init__(kind)
        self.kind = kind
        self.status = status
        self.request_id = request_id
        self.diagnostic = diagnostic


# RPC provider error strings can contain endpoint URLs, arbitrary user data, or secrets.
# Do NOT persist raw RPC error JSON, unknown message strings, URLs, or error data.
# Save safe structural fields, known revert short-codes, and hashes for grouping.
_KNOWN_REVERT_REASONS = frozenset({
    'STF', 'TF', 'LOK', 'SPL', 'IIA', 'AS', 'R', 'T', 'TLU', 'TLM',
    'TUM', 'AI', 'NP', 'LS', 'F0', 'F1', 'OLD', 'BP', 'I', 'EXPIRED',
    'TRANSFER_FAILED', 'INSUFFICIENT_LIQUIDITY', 'INSUFFICIENT_INPUT_AMOUNT',
    'INSUFFICIENT_OUTPUT_AMOUNT',
})


def _classified_message(message: Any) -> str:
    if not isinstance(message, str):
        return 'unknown'
    m = message.lower()
    if 'execution reverted' in m or 'vm exception while processing transaction: revert' in m:
        return 'execution_reverted'
    if 'insufficient liquidity' in m:
        return 'insufficient_liquidity'
    if 'transfer failed' in m or 'transferhelper' in m:
        return 'transfer_failure'
    if 'out of gas' in m:
        return 'out_of_gas'
    if 'header not found' in m or 'missing trie node' in m:
        return 'state_unavailable'
    if 'rate limit' in m or 'too many requests' in m:
        return 'rate_limited'
    return 'other'  # No raw untrusted strings in the database.


def _revert_data(error: Any, depth: int = 0) -> str | None:
    """Find only hex revert data in common JSON-RPC nested error shapes."""
    if depth > 4:
        return None
    if isinstance(error, str):
        if len(error) <= 131_074 and re.fullmatch(r'0x[0-9a-fA-F]+', error):
            return error.lower()
        return None
    if isinstance(error, dict):
        for k in ('data', 'result', 'originalError', 'error'):
            if k in error:
                found = _revert_data(error[k], depth+1)
                if found:
                    return found
    return None


def _revert_reason(hex_data: str) -> str | None:
    """Decode ABI Error(string) ONLY to recognize preapproved short revert codes."""
    if not hex_data.startswith('0x08c379a0'):
        return None
    raw = hex_data[10:]
    if len(raw) < 128 or len(raw) % 64:
        return None
    try:
        offset = int(raw[:64], 16)
        if offset > 4096 or (offset % 32):
            return None
        pos = offset * 2
        if len(raw) < pos+64:
            return None
        n = int(raw[pos:pos+64], 16)
        if n > 128 or len(raw) < pos+64+2*n:
            return None
        word = bytes.fromhex(raw[pos+64:pos+64+n*2]).decode('ascii')
        return word if word in _KNOWN_REVERT_REASONS else None
    except (ValueError, UnicodeDecodeError):
        return None


def safe_rpc_diagnostic(error: Any) -> dict:
    """Deterministic, bounded, credential-safe metadata for an RPC error.

    No provider free text or raw error data is persisted. Nonallowlisted revert
    reasons are deliberately not persisted even if technically decodable.
    """
    if not isinstance(error, dict):
        return {'message_class': 'unknown', 'data_class': 'absent'}
    out = {'message_class': _classified_message(error.get('message'))}
    code = error.get('code')
    if type(code) is int and -(2**31) <= code < 2**31:
        out['rpc_code'] = code
    h = _revert_data(error.get('data'))
    if h and len(h) >= 10 and len(h) % 2 == 0:
        out['data_class'] = 'abi_revert_data'
        out['selector'] = h[:10]
        out['data_sha256'] = hashlib.sha256(h.encode('ascii')).hexdigest()
        out['data_bytes'] = (len(h)-2)//2
        if h[:10] == '0x4e487b71' and len(h) >= 74:
            out['panic_code'] = hex(int(h[10:74], 16))
        known = _revert_reason(h)
        if known:
            out['allowlisted_reason'] = known
    else:
        out['data_class'] = 'absent_or_unrecognized'
    # Safe short code embedded in common 'execution reverted: STF' messages.
    msg = error.get('message')
    if isinstance(msg, str):
        m = re.search(r'execution reverted:\s*([A-Za-z0-9_]+)(?:$|[;\s])', msg, re.I)
        if m and m.group(1) in _KNOWN_REVERT_REASONS:
            out['allowlisted_reason'] = m.group(1)
    return out

def now_ms()->int:return time.time_ns()//1_000_000
def jdump(x:Any)->str:return json.dumps(x,sort_keys=True,separators=(',',':'))
def sha(x:Any)->str:return hashlib.sha256(jdump(x).encode()).hexdigest()
def address(x:Any)->str:
    if not isinstance(x,str) or not re.fullmatch('0x[0-9a-fA-F]{40}',x):raise DataError('Invalid address')
    return x.lower()
def is_hash(x:Any)->bool:return isinstance(x,str) and bool(re.fullmatch('0x[0-9a-fA-F]{64}',x))
def event_address(v:int)->str:
    if v>>160:raise DataError('Nonzero address padding')
    return address('0x'+f'{v:040x}')
def signed(v:int,bits:int)->int:
    raw=v&((1<<bits)-1)
    n=raw-(1<<bits) if raw&(1<<(bits-1)) else raw
    if v!=n%(1<<256):raise DataError('Bad integer sign extension')
    return n
def uvalue(v:int,bits:int)->int:
    if not 0<=v<(1<<bits):raise DataError('Integer overflow')
    return v
def parse_log_base(log:dict)->tuple[str,list[int],list[int],int,int,str,str]:
    a=address(log['address']);tt=log['topics']
    if not isinstance(tt,list) or not tt or any(not is_hash(t) for t in tt):raise DataError('Invalid log topics')
    if log.get('removed') is True:raise DataError('Removed log')
    if not is_hash(log.get('blockHash')) or not is_hash(log.get('transactionHash')):raise DataError('Missing block/transaction hash')
    try:
        block=int(log['blockNumber'],16);idx=int(log['logIndex'],16)
    except (ValueError,KeyError,TypeError):raise DataError('Invalid block/index') from None
    if block<0 or idx<0:raise DataError('Negative block/index')
    data=log.get('data')
    if not isinstance(data,str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{64})*',data):raise DataError('Invalid ABI data')
    return a,[int(t,16) for t in tt],[int(data[i:i+64],16) for i in range(2,len(data),64)],block,idx,log['blockHash'].lower(),log['transactionHash'].lower()

def decode_creation(log:dict)->dict:
    a,t,d,b,i,bh,tx=parse_log_base(log)
    if len(t)!=4 or len(d)!=2:raise DataError('Creation ABI size mismatch')
    token0,token1=event_address(t[1]),event_address(t[2])
    if a==UNI_FACTORY and t[0]==int(FACTORY_TOPICS['uniswap_v3'],16):
        kind='uniswap_v3';pool=event_address(d[1]);uvalue(t[3],24);signed(d[0],24)
        meta={'fee':t[3],'tick_spacing':signed(d[0],24)}
    elif a==AERO_FACTORY and t[0]==int(FACTORY_TOPICS['aerodrome_classic'],16):
        kind='aerodrome_classic';pool=event_address(d[0]);uvalue(t[3],1)
        meta={'stable':bool(t[3]),'factory_pool_index':d[1]}
    else:raise DataError('Creation factory/topic mismatch')
    if token0==token1 or pool=='0x'+'0'*40:raise DataError('Invalid created pool')
    return {'pool':pool,'dex':kind,'factory':a,'token0':token0,'token1':token1,
      'meta':jdump(meta),'block':b,'idx':i,'bh':bh,'tx':tx}

def decode_event(log:dict,protocol:str,expected_pool:str)->dict:
    a,t,d,b,i,bh,tx=parse_log_base(log)
    if a!=expected_pool:raise DataError('Wrong pool address')
    kind=TOPIC_EVENT.get('0x'+f'{t[0]:064x}')
    if kind not in PROTOCOL_ALLOWED.get(protocol,set()):raise DataError('Wrong event protocol/topic')
    # Raw amounts are always strings (no overflow/float rounding). No USD inferred.
    if kind=='v3_initialize':
        if len(t)!=1 or len(d)!=2:raise DataError('Bad V3 Initialize ABI')
        raw={'sqrtPriceX96':str(uvalue(d[0],160)),'tick':signed(d[1],24)}
    elif kind=='v3_mint':
        if len(t)!=4 or len(d)!=4:raise DataError('Bad V3 Mint ABI')
        raw={'owner':event_address(t[1]),'tickLower':signed(t[2],24),
            'tickUpper':signed(t[3],24),'sender':event_address(d[0]),
            'liquidity_delta':str(uvalue(d[1],128)),'amount0':str(d[2]),'amount1':str(d[3])}
    elif kind=='v3_swap':
        if len(t)!=3 or len(d)!=5:raise DataError('Bad V3 Swap ABI')
        a0,a1=signed(d[0],256),signed(d[1],256)
        raw={'sender':event_address(t[1]),'recipient':event_address(t[2]),
             'amount0_delta':str(a0),'amount1_delta':str(a1),
             'sqrtPriceX96':str(uvalue(d[2],160)),'active_liquidity':str(uvalue(d[3],128)),
             'tick':signed(d[4],24),
             'direction_token0':('token0_in' if a0>0 and a1<0 else
                                 'token0_out' if a0<0 and a1>0 else 'ambiguous')}
    elif kind=='v3_burn':
        if len(t)!=4 or len(d)!=3:raise DataError('Bad V3 Burn ABI')
        raw={'owner':event_address(t[1]),'tickLower':signed(t[2],24),'tickUpper':signed(t[3],24),
             'liquidity_delta':str(uvalue(d[0],128)),'amount0':str(d[1]),'amount1':str(d[2])}
    elif kind=='aero_mint':
        if len(t)!=2 or len(d)!=2:raise DataError('Bad Aero Mint ABI')
        raw={'sender':event_address(t[1]),'amount0':str(d[0]),'amount1':str(d[1])}
    elif kind=='aero_swap':
        if len(t)!=3 or len(d)!=4:raise DataError('Bad Aero Swap ABI')
        a0,a1,o0,o1=d
        raw={'sender':event_address(t[1]),'recipient':event_address(t[2]),
             'amount0In':str(a0),'amount1In':str(a1),'amount0Out':str(o0),'amount1Out':str(o1),
             'direction_token0':('token0_in' if a0>0 and o1>0 and a1==0 and o0==0 else
                                 'token0_out' if a1>0 and o0>0 and a0==0 and o1==0 else 'ambiguous')}
    elif kind=='aero_burn':
        if len(t)!=3 or len(d)!=2:raise DataError('Bad Aero Burn ABI')
        raw={'sender':event_address(t[1]),'recipient':event_address(t[2]),'amount0':str(d[0]),'amount1':str(d[1])}
    elif kind=='aero_sync':
        if len(t)!=1 or len(d)!=2:raise DataError('Bad Aero Sync ABI')
        raw={'reserve0_raw':str(d[0]),'reserve1_raw':str(d[1])}
    else:raise DataError('Unsupported event')
    return {'pool':a,'kind':kind,'raw':jdump(raw),'block':b,'idx':i,'bh':bh,'tx':tx}

def pct(vals:list[float],p:float)->float|None:
    if not vals:return None
    xs=sorted(vals);j=(len(xs)-1)*p/100;i=int(j)
    return round(xs[i]+(xs[min(i+1,len(xs)-1)]-xs[i])*(j-i),3)

class Transport:
    def __init__(self,url:str,timeout:float):
        u=urllib.parse.urlsplit(url)
        if u.scheme not in ('http','https') or not u.hostname or u.username or u.password:raise DataError('Bad RPC URL')
        if u.scheme!='https' and u.hostname not in ('127.0.0.1','localhost','::1'):raise DataError('RPC requires HTTPS')
        self.url=url;self.timeout=timeout;self.seq=0
    def request(self,url:str,obj:dict|None=None,gecko:bool=False):
        headers=GECKO_HEADERS if gecko else HEADERS
        if obj is None:req=urllib.request.Request(url,headers=headers)
        else:req=urllib.request.Request(url,data=jdump(obj).encode(),
            headers={**headers,'Content-Type':'application/json'},method='POST')
        try:
            with urllib.request.urlopen(req,timeout=self.timeout) as r:
                buf=r.read(2_000_001); status=r.status
            if len(buf)>2_000_000:raise TransportError('oversize_response',status)
            return json.loads(buf),status
        except urllib.error.HTTPError as e:raise TransportError('http_'+str(e.code),e.code) from None
        except (urllib.error.URLError,TimeoutError) as e:raise TransportError(type(e).__name__) from None
        except (ValueError,UnicodeDecodeError) as e:raise TransportError('invalid_json') from None
    def rpc(self,method:str,params:list):
        if method not in ('eth_chainId','eth_blockNumber','eth_getLogs','eth_getBlockByNumber','eth_call','eth_getCode'):
            raise DataError('Method not on READ ONLY whitelist')
        self.seq+=1
        data,status=self.request(self.url,{'jsonrpc':'2.0','method':method,'params':params,'id':self.seq})
        if not isinstance(data,dict) or data.get('id')!=self.seq or data.get('jsonrpc')!='2.0':raise TransportError('invalid_envelope',status)
        if data.get('error'):
            code=data['error'].get('code') if isinstance(data['error'],dict) else None
            raise TransportError('rpc_error_'+str(code if type(code) is int else 'unknown'), status, diagnostic=safe_rpc_diagnostic(data['error']))
        if 'result' not in data:raise TransportError('no_result',status)
        return data['result'],status

DDL="""
CREATE TABLE campaign(id INTEGER PRIMARY KEY CHECK(id=1),schema_v INTEGER NOT NULL,
 version TEXT NOT NULL,chain INTEGER NOT NULL,config_json TEXT NOT NULL,config_hash TEXT NOT NULL,
 started_ms INTEGER NOT NULL,deadline_ms INTEGER NOT NULL,start_block INTEGER NOT NULL,
 next_block INTEGER NOT NULL,cutoff_block INTEGER,drain_deadline_ms INTEGER,
 phase TEXT NOT NULL,completed_ms INTEGER);
CREATE TABLE requests(id INTEGER PRIMARY KEY,source TEXT NOT NULL,method TEXT NOT NULL,
 pool TEXT,started_ms INTEGER,ended_ms INTEGER,success INTEGER, http_status INTEGER,
 error_kind TEXT,result_n INTEGER,error_diag_json TEXT);
CREATE INDEX requests_budget ON requests(source,started_ms);
CREATE TABLE scanned_ranges(id INTEGER PRIMARY KEY,first_block INTEGER NOT NULL,
 last_block INTEGER NOT NULL,observed_ms INTEGER NOT NULL, creation_events INTEGER NOT NULL,
 pool_events INTEGER NOT NULL,UNIQUE(first_block,last_block));
CREATE TABLE pools(pool TEXT PRIMARY KEY,dex TEXT NOT NULL,factory TEXT NOT NULL,
 token0 TEXT NOT NULL,token1 TEXT NOT NULL,meta_json TEXT NOT NULL,
 block_number INTEGER NOT NULL,block_hash TEXT NOT NULL,block_time_ms INTEGER NOT NULL,
 creation_tx TEXT NOT NULL,creation_index INTEGER NOT NULL,first_seen_ms INTEGER NOT NULL,
 initial_state TEXT NOT NULL,current_state TEXT NOT NULL,
 UNIQUE(creation_tx,creation_index));
CREATE INDEX pool_current_state_idx ON pools(current_state,first_seen_ms);
CREATE TABLE events(id INTEGER PRIMARY KEY,pool TEXT NOT NULL REFERENCES pools(pool),
 event_kind TEXT NOT NULL,block_number INTEGER NOT NULL,block_hash TEXT NOT NULL,
 block_time_ms INTEGER NOT NULL,tx_hash TEXT NOT NULL,log_index INTEGER NOT NULL,
 observed_ms INTEGER NOT NULL,raw_json TEXT NOT NULL,within_watch INTEGER NOT NULL,
 UNIQUE(tx_hash,log_index));
CREATE INDEX events_pool_kind ON events(pool,event_kind,observed_ms);
CREATE TABLE provider_state(pool TEXT NOT NULL REFERENCES pools(pool),source TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,next_due_ms INTEGER NOT NULL,
 first_listed_ms INTEGER,first_priced_ms INTEGER,
 last_fingerprint TEXT,changed_count INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(pool,source));
CREATE TABLE provider_checks(id INTEGER PRIMARY KEY,pool TEXT NOT NULL REFERENCES pools(pool),
 source TEXT NOT NULL,request_id INTEGER,observed_ms INTEGER NOT NULL,
 listed INTEGER NOT NULL,priced INTEGER NOT NULL,price_raw TEXT,liquidity_usd REAL,
 note TEXT NOT NULL, fingerprint TEXT);
CREATE INDEX checks_source_pool ON provider_checks(source,pool,observed_ms);
CREATE TABLE issues(id INTEGER PRIMARY KEY,at_ms INTEGER NOT NULL,category TEXT NOT NULL,detail TEXT NOT NULL);
CREATE TABLE pool_cohorts(pool TEXT PRIMARY KEY REFERENCES pools(pool),cohort TEXT NOT NULL,
 anchor TEXT,target TEXT,reason TEXT NOT NULL,first_swap_seen_ms INTEGER,quote_eligible INTEGER NOT NULL);
CREATE TABLE token_metadata(token TEXT PRIMARY KEY,decimals INTEGER,attempts INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'pending',error_kind TEXT,request_id INTEGER,observed_ms INTEGER,
 state_block INTEGER,next_due_ms INTEGER NOT NULL DEFAULT 0);
CREATE TABLE quote_schedule(id INTEGER PRIMARY KEY,pool TEXT NOT NULL REFERENCES pools(pool),
 side TEXT NOT NULL,size_key TEXT NOT NULL,buy_delay_s INTEGER NOT NULL,
 horizon_min INTEGER NOT NULL,due_ms INTEGER,created_ms INTEGER NOT NULL,
 status TEXT NOT NULL,request_started_ms INTEGER,response_received_ms INTEGER,
 reason TEXT,buy_job_id INTEGER,state_block INTEGER,head_at_request INTEGER,request_id INTEGER REFERENCES requests(id),UNIQUE(pool,side,size_key,buy_delay_s,horizon_min));
CREATE INDEX quote_sched_due ON quote_schedule(status,due_ms);
CREATE TABLE quotes(id INTEGER PRIMARY KEY,job_id INTEGER NOT NULL UNIQUE REFERENCES quote_schedule(id),
 pool TEXT NOT NULL,side TEXT NOT NULL,anchor TEXT NOT NULL,target TEXT NOT NULL,
 amount_in_raw TEXT NOT NULL,amount_out_raw TEXT NOT NULL,observed_ms INTEGER NOT NULL,
 request_id INTEGER NOT NULL,state_block INTEGER NOT NULL,state_hash TEXT NOT NULL,
 state_time_ms INTEGER NOT NULL,head_at_request INTEGER NOT NULL,
 quoter TEXT NOT NULL,sqrt_price_after TEXT,ticks_crossed INTEGER,quoter_gas_estimate TEXT,
 spot_token1_per_token0 TEXT,active_liquidity TEXT);
CREATE TABLE pool_state(pool TEXT PRIMARY KEY REFERENCES pools(pool),status TEXT NOT NULL,
 protocol TEXT NOT NULL, state_block INTEGER, state_hash TEXT,
 first_started_ms INTEGER, last_observed_ms INTEGER, sqrt_price_x96 TEXT,
 spot_token1_per_token0 TEXT, active_liquidity_raw TEXT, reserve0_raw TEXT,reserve1_raw TEXT,
 error_kind TEXT);
CREATE TABLE rpc_state(id INTEGER PRIMARY KEY CHECK(id=1),call_fail_streak INTEGER NOT NULL DEFAULT 0,
 quotes_suspended INTEGER NOT NULL DEFAULT 0,call_attempts INTEGER NOT NULL DEFAULT 0);
INSERT INTO rpc_state(id) VALUES (1);
"""

def settings(a:argparse.Namespace)->dict:
    # Endpoints/API secrets omitted from DB configuration and logs.
    return {k:getattr(a,k) for k in ('duration_hours','poll_seconds','confirmations',
        'max_blocks_per_range','max_ranges_per_cycle','max_active_pools','watch_minutes',
        'drain_minutes','max_pool_addresses_per_query','dex_requests_per_minute',
        'gecko_requests_per_minute','dex_spacing_seconds','gecko_spacing_seconds',
        'max_provider_calls_per_cycle','http_timeout','no_providers','max_consecutive_scan_errors','no_quotes','eth_call_per_minute','max_quote_calls_per_cycle','max_quote_lateness_seconds')}

def connect(path:Path,read_only:bool=False)->sqlite3.Connection:
    if read_only:
        if not path.is_file():raise DataError('Database missing; no file created')
        c=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=8)
        c.execute('PRAGMA query_only=ON')
    else:
        c=sqlite3.connect(path,timeout=8)
        c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=FULL')
    c.execute('PRAGMA foreign_keys=ON');c.execute('PRAGMA busy_timeout=8000')
    c.row_factory=sqlite3.Row
    return c

def create_db(path:Path,a:argparse.Namespace,tip:int,t:int)->sqlite3.Connection:
    if path.exists():raise DataError('Fresh run refuses existing database')
    if not path.parent.is_dir():raise DataError('Create database parent directory first')
    con=connect(path)
    try:
        con.executescript(DDL)
        conf=settings(a)
        con.execute('INSERT INTO campaign VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (SCHEMA,VERSION,CHAIN,jdump(conf),sha(conf),t,t+round(a.duration_hours*3_600_000),
             tip+1,tip+1,None,None,'running',None))
        con.commit()
    except Exception:
        con.close();raise
    return con

def campaign(c):
    r=c.execute('SELECT * FROM campaign WHERE id=1').fetchone()
    if r is None or (r['schema_v'],r['version'],r['chain'])!=(SCHEMA,VERSION,CHAIN):
        raise DataError('Incompatible database')
    return r

def call(c, transport: Transport, source: str, method: str, params=None, pool=None):
    began = now_ms()
    data = None
    http = None
    err = None
    diagnostic = None
    success = False
    try:
        if source == 'rpc':
            data,http = transport.rpc(method, params or [])
        elif source == 'dex':
            data,http = transport.request(DEX_URL+pool)
        elif source == 'gecko':
            data,http = transport.request(GECKO_URL+pool,gecko=True)
        else:
            raise DataError('Invalid request source')
        success = True
    except TransportError as e:
        err = e.kind
        http = e.status
        if source == 'rpc' and method == 'eth_call' and e.diagnostic:
            diagnostic = e.diagnostic
    except (OSError,DataError) as e:
        err = type(e).__name__
    ended = now_ms()
    cur = c.execute('''INSERT INTO requests
      (source,method,pool,started_ms,ended_ms,success,http_status,error_kind,result_n,error_diag_json)
      VALUES (?,?,?,?,?,?,?,?,?,?)''',
       (source,method,pool,began,ended,int(success),http,err,
        len(data) if isinstance(data,list) else None,
        jdump(diagnostic) if diagnostic else None))
    c.commit()
    if not success:
        # Crucially retain the database ID on the raised error. Quote job code
        # links failed eth_call requests just like successfully decoded quotes.
        raise TransportError(err or 'unknown',http,request_id=cur.lastrowid,diagnostic=diagnostic)
    return data,ended,cur.lastrowid

def tip(c,t)->int:
    result,_,_=call(c,t,'rpc','eth_blockNumber')
    try:n=int(result,16)
    except (TypeError,ValueError):raise DataError('Invalid blockNumber') from None
    if n<0:raise DataError('Negative tip')
    return n

def block_info(c,t,num:int,cache:dict):
    if num not in cache:
        x,_,_=call(c,t,'rpc','eth_getBlockByNumber',[hex(num),False])
        if not isinstance(x,dict) or not is_hash(x.get('hash')):raise DataError('Invalid block result')
        if int(x['number'],16)!=num:raise DataError('Wrong block number')
        cache[num]=(int(x['timestamp'],16)*1000,x['hash'].lower())
    return cache[num]

def get_logs(c,t,first:int,last:int,addresses:list[str],topic_filter:list[str]):
    if not addresses:return [],now_ms()
    x,ms,_=call(c,t,'rpc','eth_getLogs',[{'fromBlock':hex(first),'toBlock':hex(last),
        'address':addresses,'topics':[topic_filter]}])
    if not isinstance(x,list):raise DataError('getLogs non-list response')
    return x,ms

def provider_fields(source:str,data:Any,pool:str)->dict:
    result={'listed':False,'priced':False,'price_raw':None,'liq':None,
            'fingerprint':None,'note':'not_listed'}
    entry=None
    if source=='dex':
        if not isinstance(data,dict) or not (data.get('pairs') is None or isinstance(data.get('pairs'),list)):
            raise DataError('Bad Dex payload')
        for q in data.get('pairs') or []:
            if isinstance(q,dict) and str(q.get('chainId','')).lower()=='base' and str(q.get('pairAddress','')).lower()==pool:
                entry=q;break
        if entry:
            result['price_raw']=entry.get('priceUsd')
            l=entry.get('liquidity');result['liq']=l.get('usd') if isinstance(l,dict) else None
            relevant={k:entry.get(k) for k in ('priceUsd','liquidity','volume','txns','priceChange')}
    elif source=='gecko':
        if not isinstance(data,dict):raise DataError('Bad Gecko payload')
        val=data.get('data')
        if isinstance(val,dict) and str(val.get('id','')).lower()=='base_'+pool:
            attrs=val.get('attributes')
            if isinstance(attrs,dict) and str(attrs.get('address','')).lower()==pool:
                entry=attrs
        if entry:
            result['price_raw']=entry.get('base_token_price_usd')
            result['liq']=entry.get('reserve_in_usd')
            relevant={k:entry.get(k) for k in ('base_token_price_usd','reserve_in_usd',
                                                    'volume_usd','transactions','price_change_percentage')}
    else:raise DataError('Unsupported provider')
    if entry is not None:
        result['listed']=True
        try:
            p=float(result['price_raw']);result['priced']=math.isfinite(p) and p>0
            if not result['priced']:result['price_raw']=None
        except (ValueError,TypeError,OverflowError):result['price_raw']=None
        try:
            l=float(result['liq'])
            result['liq']=l if math.isfinite(l) and l>=0 else None
        except (ValueError,TypeError,OverflowError):result['liq']=None
        result['price_raw']=str(result['price_raw']) if result['priced'] else None
        result['fingerprint']=sha(relevant)
        result['note']='priced' if result['priced'] else 'listed_unpriced'
    return result

def provider_checks(c,t,a):
    if a.no_providers:return 0
    used=0
    for source,limit,spacing in (('dex',a.dex_requests_per_minute,a.dex_spacing_seconds),
                                ('gecko',a.gecko_requests_per_minute,a.gecko_spacing_seconds)):
        while limit>0 and used<a.max_provider_calls_per_cycle:
            moment=now_ms()
            n=c.execute('SELECT COUNT(*) FROM requests WHERE source=? AND started_ms>?',
                        (source,moment-60_000)).fetchone()[0]
            if n>=limit:break
            q=c.execute('''SELECT s.*,p.first_seen_ms FROM provider_state s JOIN pools p ON p.pool=s.pool
                       WHERE s.source=? AND p.initial_state='tracked' AND
                             p.first_seen_ms+?>? AND s.next_due_ms<=?
                       ORDER BY s.next_due_ms,p.first_seen_ms LIMIT 1''',
                       (source,a.watch_minutes*60_000,moment,moment)).fetchone()
            if q is None:break
            outcome={'listed':False,'priced':False,'price_raw':None,'liq':None,
                     'fingerprint':None,'note':'request_failed'}
            rid=None
            try:
                data,received,rid=call(c,t,source,'pool_lookup',pool=q['pool'])
                outcome=provider_fields(source,data,q['pool'])
            except (TransportError,DataError) as e:
                received=now_ms()
                outcome['note']='error_'+(e.kind if isinstance(e,TransportError) else 'invalid_payload')
            changed=int(outcome['fingerprint'] is not None and
                        q['last_fingerprint'] is not None and outcome['fingerprint']!=q['last_fingerprint'])
            with c:
                c.execute('INSERT INTO provider_checks(pool,source,request_id,observed_ms,listed,priced,price_raw,liquidity_usd,note,fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?)',
                  (q['pool'],source,rid,received,int(outcome['listed']),int(outcome['priced']),
                   outcome['price_raw'],outcome['liq'],outcome['note'],outcome['fingerprint']))
                c.execute('''UPDATE provider_state SET attempts=attempts+1,next_due_ms=?,
                     first_listed_ms=CASE WHEN first_listed_ms IS NULL AND ? THEN ? ELSE first_listed_ms END,
                     first_priced_ms=CASE WHEN first_priced_ms IS NULL AND ? THEN ? ELSE first_priced_ms END,
                     last_fingerprint=COALESCE(?,last_fingerprint),changed_count=changed_count+?
                     WHERE pool=? AND source=?''',
                  (received+spacing*1000,int(outcome['listed']),received,int(outcome['priced']),received,
                   outcome['fingerprint'],changed,q['pool'],source))
            used+=1
    return used

def persist_range(c,created,events,first,last,seen,a):
    r=campaign(c)
    if first!=r['next_block'] or last<first:raise DataError('Noncontiguous or inverted block range')
    creations=sorted(created,key=lambda x:(x['block'],x['idx']))
    pool_map={z['pool']:z for z in creations}
    if len(pool_map)!=len(creations):raise DataError('Duplicate creation pool')
    with c:
        c.execute('UPDATE pools SET current_state="expired" WHERE current_state="tracked" AND first_seen_ms+?<=?',
                  (a.watch_minutes*60_000,seen))
        active=c.execute("SELECT COUNT(*) FROM pools WHERE current_state='tracked'").fetchone()[0]
        tracked={p['pool'] for p in c.execute("SELECT pool FROM pools WHERE current_state='tracked'")}
        admitted=0;excluded=0
        for x in creations:
            if not first<=x['block']<=last:raise DataError('Creation outside scanned range')
            if c.execute('SELECT 1 FROM pools WHERE pool=?',(x['pool'],)).fetchone():
                raise DataError('Pool already exists; possible reorg or duplication')
            prospective=(r['phase']=='running' and x.get('observed_ms',seen)<r['deadline_ms'] and
                        x['block_time_ms']<=r['deadline_ms'])
            state=('tracked' if prospective and active<a.max_active_pools else
                   'not_admitted_capacity' if prospective else 'recovered_after_cutoff')
            if state=='tracked':active+=1;admitted+=1;tracked.add(x['pool'])
            else:excluded+=1
            c.execute('INSERT INTO pools VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
              (x['pool'],x['dex'],x['factory'],x['token0'],x['token1'],x['meta'],
               x['block'],x['bh'],x['block_time_ms'],x['tx'],x['idx'],x.get('observed_ms',seen),state,state))
            cohort,anchor,target = classify_pair(x['token0'],x['token1'])
            eligible=int(state=='tracked' and anchor is not None and not a.no_quotes)
            reason=('direct_anchor' if eligible else
                    'unsupported_route_v03a' if anchor is None else
                    state if state!='tracked' else 'quotes_disabled')
            c.execute('INSERT INTO pool_cohorts(pool,cohort,anchor,target,reason,quote_eligible) VALUES(?,?,?,?,?,?)',
                      (x['pool'],cohort,anchor,target,reason,eligible))
            if state=='tracked' and not a.no_providers:
                for source in ('dex','gecko'):
                    c.execute('INSERT INTO provider_state(pool,source,next_due_ms) VALUES(?,?,?)',
                             (x['pool'],source,x.get('observed_ms',seen)))
        n_events=0
        for ev in sorted(events,key=lambda e:(e['block'],e['idx'])):
            if not first<=ev['block']<=last:raise DataError('Event outside range')
            p=c.execute('SELECT * FROM pools WHERE pool=?',(ev['pool'],)).fetchone()
            if p is None:raise DataError('Event from unknown pool')
            if p['initial_state']!='tracked':continue  # admitted before request, but no longer eligible at response
            if ev['block']<p['block_number'] or (ev['block']==p['block_number'] and ev['idx']<=p['creation_index']):
                raise DataError('Event before pool creation')
            if ev['block_time_ms'] < p['block_time_ms']:raise DataError('Event block time precedes creation')
            # Observation comes strictly from when the node responded, not from retrospectively known block time.
            # Preserve late observations in event table but mark them unqualified for watch-time metrics.
            event_observed=ev.get('observed_ms',seen)
            qualifies=int(event_observed<p['first_seen_ms']+a.watch_minutes*60_000)
            c.execute('INSERT INTO events(pool,event_kind,block_number,block_hash,block_time_ms,tx_hash,log_index,observed_ms,raw_json,within_watch) VALUES(?,?,?,?,?,?,?,?,?,?)',
               (ev['pool'],ev['kind'],ev['block'],ev['bh'],ev['block_time_ms'],ev['tx'],ev['idx'],event_observed,ev['raw'],qualifies))
            n_events+=1
        c.execute('INSERT INTO scanned_ranges(first_block,last_block,observed_ms,creation_events,pool_events) VALUES(?,?,?,?,?)',
                 (first,last,seen,len(creations),n_events))
        c.execute('UPDATE campaign SET next_block=? WHERE id=1',(last+1,))
    return admitted,excluded,n_events

def scan_ranges(c,t,a,safe):
    evcount=0;admitted=0;excluded=0;failed=False
    for _ in range(a.max_ranges_per_cycle):
        r=campaign(c);first=r['next_block']
        if first>safe:break
        last=min(safe,first+a.max_blocks_per_range-1)
        cache={}
        try:
            cut=r['cutoff_block']
            # During drain, factory discovery is restricted to the frozen cutoff block;
            # pool events continue for existing prospects in the fixed followup.
            if cut is not None and first>cut:
                created=[]
            else:
                cr_end=min(last,cut) if cut is not None else last
                factory_logs,seen_factory=get_logs(c,t,first,cr_end,[UNI_FACTORY,AERO_FACTORY],list(FACTORY_TOPICS.values()))
                created=[decode_creation(q) for q in factory_logs]
                for z in created:z['observed_ms']=seen_factory
                for z in created:
                    ts,bh=block_info(c,t,z['block'],cache)
                    if bh!=z['bh']:raise DataError('Noncanonical creation block')
                    z['block_time_ms']=ts
            # Preselect newly eligible pools without lookahead beyond this range.
            # Final admission is rechecked transactionally in persist_range.
            observed_now=now_ms()
            active=[(p['pool'],p['dex']) for p in c.execute('''SELECT pool,dex FROM pools
                    WHERE initial_state='tracked' AND first_seen_ms+?>?''',
                    (a.watch_minutes*60_000,observed_now))]
            room=max(0,a.max_active_pools-len(active))
            prospective=(r['phase']=='running' and observed_now<r['deadline_ms'])
            for z in sorted(created,key=lambda q:(q['block'],q['idx'])):
                if prospective and z['block_time_ms']<=r['deadline_ms'] and room>0:
                    active.append((z['pool'],z['dex']));room-=1
            if len(active)>a.max_pool_addresses_per_query:
                raise DataError('Pool batch beyond configured size; no cursor advance')
            requested=dict(active)
            events=[]
            if active:
                pool_logs,seen_pool=get_logs(c,t,first,last,list(requested),list(EVT_TOPICS.values()))
                for log in pool_logs:
                    pool=address(log['address'])
                    if pool not in requested:raise DataError('Unrequested pool event')
                    q=decode_event(log,requested[pool],pool)
                    ts,bh=block_info(c,t,q['block'],cache)
                    if bh!=q['bh']:raise DataError('Noncanonical pool event block')
                    q['block_time_ms']=ts
                    q['observed_ms']=seen_pool
                    events.append(q)
            seen=now_ms()
            n,k,m=persist_range(c,created,events,first,last,seen,a)
            admitted+=n;excluded+=k;evcount+=m
        except (TransportError,DataError,sqlite3.Error,KeyError,TypeError,ValueError) as e:
            # Leave cursor where it was: all logs in this range are retryable.
            c.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                      (now_ms(),'scan_error',e.kind if isinstance(e,TransportError) else type(e).__name__))
            c.commit()
            print('SCAN WARNING: cursor unchanged; '+(e.kind if isinstance(e,TransportError) else type(e).__name__),flush=True)
            failed=True
            break
    return admitted,excluded,evcount,failed

def report(c)->dict:
    r=campaign(c)
    x={'version':VERSION,'network':'base','chain_id':CHAIN,
       'campaign':{k:r[k] for k in ('phase','started_ms','deadline_ms','start_block','next_block','cutoff_block','completed_ms')},
       'scope':['uniswap_v3','aerodrome_classic'],
       'scanned_ranges':c.execute('SELECT COUNT(*) FROM scanned_ranges').fetchone()[0],
       'scanned_blocks':c.execute('SELECT COALESCE(SUM(last_block-first_block+1),0) FROM scanned_ranges').fetchone()[0],
       'creation_events_by_dex':{},'admission_initial':{},'event_counts_all':{},'event_counts_within_watch':{},
       'timing':{},'provider':{},'paired_observation':{},'issues':{},'rpc_requests':0,'rpc_errors':0,
       'interpretation':['Two factory contracts only, not all Base markets',
          'Observed time = VPS response receipt after confirmations and HTTP delays',
          'Block timestamp is NOT first-known-to-VPS timestamp',
          'Token0 direction is NOT necessarily buying the target token',
          'Raw amounts lack token decimals/quote conversion; no USD volume, execution price or trading PnL',
          'Missing/capacity-excluded pool followups cannot be assumed to be zero or healthy',
          'Paired on-chain/provider lead compares timestamps of two different sampled observations, NOT executable prices',
          'Quoter results are counterfactual eth_call estimates, not trades or proof of token sellability',
          'V0.3a has no paper PnL, expectancy, win rate or realized fills']}
    for key,n in c.execute('SELECT dex,COUNT(*) FROM pools GROUP BY dex'):x['creation_events_by_dex'][key]=n
    for key,n in c.execute('SELECT initial_state,COUNT(*) FROM pools GROUP BY initial_state'):x['admission_initial'][key]=n
    for key,n in c.execute('SELECT event_kind,COUNT(*) FROM events GROUP BY event_kind'):x['event_counts_all'][key]=n
    for key,n in c.execute('SELECT event_kind,COUNT(*) FROM events WHERE within_watch=1 GROUP BY event_kind'):
        x['event_counts_within_watch'][key]=n
    delays=[(seen-ts)/1000 for seen,ts in c.execute('SELECT first_seen_ms,block_time_ms FROM pools')]
    x['timing']['creation_block_to_vps_lag_s']={'n':len(delays),'p50':pct(delays,50),'p95':pct(delays,95),
                                                'negative_clock_skew_count':sum(z<0 for z in delays)}
    event_delays=[(seen-ts)/1000 for seen,ts in c.execute(
        'SELECT observed_ms,block_time_ms FROM events WHERE within_watch=1')]
    x['timing']['pool_event_block_to_vps_lag_s']={
        'n':len(event_delays),'p50':pct(event_delays,50),'p95':pct(event_delays,95),
        'negative_clock_skew_count':sum(z<0 for z in event_delays)}
    for category,pattern in [('first_onchain_swap','%_swap'),('first_onchain_mint','%_mint')]:
        # Each pool contributes at most one first observed eligible event.
        vals=[]
        for seen,first in c.execute('''SELECT p.first_seen_ms, MIN(e.observed_ms)
              FROM pools p JOIN events e ON e.pool=p.pool AND e.within_watch=1
              WHERE p.initial_state='tracked' AND e.event_kind LIKE ?
              GROUP BY p.pool''',(pattern,)):
            vals.append((first-seen)/1000)
        x['timing'][category+'_delay_after_discovery_s']={'n':len(vals),'p50':pct(vals,50),'p95':pct(vals,95)}
    for source in ('dex','gecko'):
        rows=list(c.execute('''SELECT p.first_seen_ms,s.attempts,s.first_listed_ms,s.first_priced_ms,s.changed_count
            FROM provider_state s JOIN pools p ON p.pool=s.pool WHERE s.source=?''',(source,)))
        listed=[(r['first_listed_ms']-r['first_seen_ms'])/1000 for r in rows if r['first_listed_ms'] is not None]
        priced=[(r['first_priced_ms']-r['first_seen_ms'])/1000 for r in rows if r['first_priced_ms'] is not None]
        x['provider'][source]={'monitored_pools':len(rows),'never_polled':sum(r['attempts']==0 for r in rows),
             'first_listed':len(listed),'first_priced':len(priced),'listing_lag_p50_s':pct(listed,50),
             'listing_lag_p95_s':pct(listed,95),'price_lag_p50_s':pct(priced,50),
             'price_lag_p95_s':pct(priced,95),'changed_payloads':sum(r['changed_count'] for r in rows),
             'calls':c.execute('SELECT COUNT(*) FROM requests WHERE source=?',(source,)).fetchone()[0],
             'request_errors':c.execute('SELECT COUNT(*) FROM requests WHERE source=? AND success=0',(source,)).fetchone()[0]}
    # Paired comparison avoids treating population medians as a head-to-head timing result.
    # Positive lead: first on-chain event OBSERVED before first provider PRICE OBSERVED.
    # This is a comparison of sampling timestamps, NOT proof of earlier execution.
    for source in ('dex','gecko'):
        for event_type,pattern in (('swap','%_swap'),('mint','%_mint')):
            pairs=list(c.execute('''SELECT s.first_priced_ms,MIN(e.observed_ms) AS first_chain_ms
                FROM pools p
                JOIN provider_state s ON s.pool=p.pool AND s.source=?
                JOIN events e ON e.pool=p.pool AND e.within_watch=1 AND e.event_kind LIKE ?
                WHERE p.initial_state='tracked'
                GROUP BY p.pool,s.first_priced_ms''',(source,pattern)))
            leads=[(price-event_ms)/1000 for price,event_ms in pairs if price is not None]
            key=source+'_'+event_type
            x['paired_observation'][key]={
                'pool_with_observed_event':len(pairs),
                'both_event_and_provider_price':len(leads),
                'provider_price_not_observed':sum(price is None for price,event_ms in pairs),
                'onchain_observed_before_provider':sum(z>0 for z in leads),
                'provider_observed_before_onchain':sum(z<0 for z in leads),
                'observed_simultaneously':sum(z==0 for z in leads),
                'provider_minus_onchain_p50_s':pct(leads,50),
                'provider_minus_onchain_p95_s':pct(leads,95)}
    for kind,detail,n in c.execute('SELECT category,detail,COUNT(*) FROM issues GROUP BY category,detail'):
        x['issues'][kind+'_'+detail]=n
    rpcs=c.execute("SELECT COUNT(*),COALESCE(SUM(success=0),0) FROM requests WHERE source='rpc'").fetchone()
    x['rpc_requests']=rpcs[0];x['rpc_errors']=rpcs[1]
    x['provider_errors_by_source']={}
    for src,err,n in c.execute('''SELECT source,error_kind,COUNT(*) FROM requests
       WHERE source IN ('dex','gecko') AND success=0 GROUP BY source,error_kind'''):
        x['provider_errors_by_source'][src+':'+str(err)]=n
    # Completeness indicators at pool level rather than only the pool events that happened to be seen.
    x['followup']={}
    for label,kind in (('swaps','%_swap'),('mints','%_mint')):
        x['followup'][label+'_with_at_least_one_event']=c.execute('''SELECT COUNT(*) FROM pools p
            WHERE p.initial_state='tracked' AND EXISTS (SELECT 1 FROM events e
              WHERE e.pool=p.pool AND e.within_watch=1 AND e.event_kind LIKE ?)''',(kind,)).fetchone()[0]
    x['followup']['pool_windows_started']=c.execute("SELECT COUNT(*) FROM pools WHERE initial_state='tracked'").fetchone()[0]
    x['followup']['pool_windows_expired']=c.execute("SELECT COUNT(*) FROM pools WHERE initial_state='tracked' AND current_state='expired'").fetchone()[0]
    x['quote_feasibility']=quote_report(c)
    return x

def run(a,t:Transport|None=None):
    transport=t or Transport(a.rpc_url,a.http_timeout)
    path=Path(a.db)
    if a.resume:
        c=connect(path) if path.is_file() else None
        if c is None:raise DataError('Cannot resume nonexistent DB')
        r=campaign(c)
        if r['config_hash']!=sha(settings(a)):
            c.close();raise DataError('Resume settings mismatch')
        if r['phase'] in ('complete','incomplete_backlog'):
            c.close();raise DataError('Cannot resume finalized campaign')
        with c:
            c.execute('UPDATE quote_schedule SET status="censored",reason="crash_unknown_result" WHERE status="in_progress"')
    else:
        if path.exists():raise DataError('A new run must use a new DB filename')
        c=None
    try:
        ident,_=transport.rpc('eth_chainId',[])
        if int(ident,16)!=CHAIN:raise DataError('Not Base mainnet (chainId 8453)')
        if c is None:
            h,_=transport.rpc('eth_blockNumber',[])
            c=create_db(path,a,int(h,16),now_ms())
        print(f'DEX EXECUTABILITY FEASIBILITY V{VERSION} READ ONLY | chain=8453 | DB={a.db} | start_block={campaign(c)["start_block"]}',flush=True)
        loops=0
        consecutive_scan_errors=0
        while True:
            loops+=1
            current=now_ms();r=campaign(c)
            try:
                head=tip(c,transport)
            except (TransportError, DataError, sqlite3.Error) as e:
                # A tip lookup failure is just as serious as a logs failure.
                # Do not skip intervals or let an unhandled exception leave a
                # campaign that appears merely "running" forever.
                category=e.kind if isinstance(e,TransportError) else type(e).__name__
                with c:
                    c.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                              (now_ms(),'scan_error','tip_'+category))
                consecutive_scan_errors+=1
                print('SCAN WARNING: head unavailable; cursor unchanged; '+category,flush=True)
                if consecutive_scan_errors>=a.max_consecutive_scan_errors:
                    with c:
                        c.execute('UPDATE campaign SET phase="incomplete_backlog",completed_ms=? WHERE id=1',
                                  (now_ms(),))
                        c.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                                  (now_ms(),'campaign_halt','max_consecutive_scan_errors'))
                    finalize_quote_jobs(c, 'incomplete_backlog_scan_failure')
                    print(f'PILOT HALTED: {consecutive_scan_errors} consecutive RPC failures; cursor not advanced',flush=True)
                    break
                if a.once:break
                time.sleep(a.poll_seconds)
                continue
            safe=max(0,head-a.confirmations)
            if r['phase']=='running' and current>=r['deadline_ms']:
                with c:
                    c.execute('UPDATE campaign SET phase="draining",cutoff_block=?,drain_deadline_ms=? WHERE id=1',
                              (safe,r['deadline_ms']+a.drain_minutes*60_000))
            cursor_before=campaign(c)['next_block']
            admitted,excluded,evcount,scan_failed=scan_ranges(c,transport,a,safe)
            if scan_failed:
                consecutive_scan_errors+=1
            elif campaign(c)['next_block']>cursor_before:
                # Only a completed block range proves the logs RPC recovered.
                # A no-work iteration must not erase consecutive failures.
                consecutive_scan_errors=0
            if scan_failed and consecutive_scan_errors>=a.max_consecutive_scan_errors:
                # Fail closed after persistent RPC failure: never label this campaign complete.
                with c:
                    c.execute('UPDATE campaign SET phase="incomplete_backlog",completed_ms=? WHERE id=1',(now_ms(),))
                    c.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                              (now_ms(),'campaign_halt','max_consecutive_scan_errors'))
                finalize_quote_jobs(c, 'incomplete_backlog_scan_failure')
                print(f'PILOT HALTED: {consecutive_scan_errors} consecutive scan failures; cursor not advanced',flush=True)
                break
            # Discovery and committed raw logs always have priority over metadata/quotes.
            if not a.no_quotes:
                initialize_quote_schedule(c)
                if not quote_suspended(c):
                    process_quote_jobs(c,transport,a,head)
            if campaign(c)['phase']=='running' or (campaign(c)['phase']=='draining' and now_ms()<campaign(c)['drain_deadline_ms']):
                providers=provider_checks(c,transport,a)
            else:providers=0
            with c:
                c.execute('UPDATE pools SET current_state="expired" WHERE current_state="tracked" AND first_seen_ms+?<=?',
                          (a.watch_minutes*60_000,now_ms()))
            r=campaign(c)
            if r['phase']=='draining' and now_ms()>=r['drain_deadline_ms']:
                # Every block through the drain cutoff must be scanned. A lagging
                # cursor is incomplete, NOT silently complete or eligible for backtests.
                complete = r['next_block']>safe
                with c:c.execute('UPDATE campaign SET phase=?,completed_ms=? WHERE id=1',
                                  ('complete' if complete else 'incomplete_backlog',now_ms()))
                finalize_quote_jobs(c, 'campaign_end')
            print(f'loop={loops} phase={campaign(c)["phase"]} next_block={campaign(c)["next_block"]} '
                  f'new_tracked={admitted} excluded={excluded} new_pool_events={evcount} provider_calls={providers}',flush=True)
            if a.once or campaign(c)['phase'] in ('complete','incomplete_backlog'):break
            time.sleep(a.poll_seconds)
        print('Final/current report:',jdump(report(c)),flush=True)
        return 2 if campaign(c)['phase']=='incomplete_backlog' else 0
    finally:
        if c is not None:c.close()

# ---- V0.3a: quote feasibility only, never execution or hypothetical PnL. ----

def classify_pair(token0: str, token1: str) -> tuple[str, str | None, str | None]:
    a, b = address(token0), address(token1)
    if a in ANCHORS.values() and b in ANCHORS.values():
        return 'both_anchors', None, None
    if a == USDC or b == USDC:
        return 'direct_usdc', USDC, b if a == USDC else a
    if a == WETH or b == WETH:
        return 'direct_weth', WETH, b if a == WETH else a
    return 'non_anchor', None, None


def selector(signature: str) -> str:
    return '0x' + keccak256(signature.encode())[:4].hex()


def abi_word(value: int) -> str:
    if not 0 <= value < 2**256:
        raise DataError('ABI uint256 outside range')
    return f'{value:064x}'


def abi_address(a: str) -> str:
    return abi_word(int(address(a), 16))


def abi_data(signature: str, *values: int | str) -> str:
    return selector(signature) + ''.join(abi_address(x) if isinstance(x, str) else abi_word(x) for x in values)


def abi_output(hex_output: str, words: int) -> list[int]:
    if not isinstance(hex_output, str) or not re.fullmatch('0x(?:[0-9a-fA-F]{64})*', hex_output):
        raise DataError('Malformed eth_call ABI output')
    if len(hex_output) != 2 + 64 * words:
        raise DataError('Unexpected eth_call result size')
    return [int(hex_output[2+i*64:2+(i+1)*64], 16) for i in range(words)]


def parse_v3_quote(raw: str) -> dict:
    v = abi_output(raw, 4)
    if v[0] <= 0 or not 0 < v[1] < 2**160 or v[2] >= 2**32:
        raise DataError('QuoterV2 returned zero/malformed estimate')
    return {'amount_out': v[0], 'sqrt_after': v[1], 'ticks': v[2], 'quoter_gas': v[3]}


def parse_aero_quote(raw: str) -> dict:
    v = abi_output(raw, 1)
    if v[0] <= 0:
        raise DataError('Aerodrome quote returned zero output')
    return {'amount_out': v[0], 'sqrt_after': None, 'ticks': None, 'quoter_gas': None}


def raw_for_size(anchor: str, size: str, decimals: int) -> int:
    if not 0 <= decimals <= 36:
        raise DataError('Token decimals out of accepted range')
    q = Decimal(size) * (10 ** decimals)
    if q != q.to_integral_value() or q <= 0 or q >= 2**256:
        raise DataError('Size cannot be encoded exactly in raw units')
    return int(q)


def price_token1_per_token0(sqrt_price_x96: int, dec0: int, dec1: int) -> str:
    if not 0 < sqrt_price_x96 < 2**160 or not 0 <= dec0 <= 36 or not 0 <= dec1 <= 36:
        raise DataError('Invalid spot price inputs')
    with localcontext() as ctx:
        ctx.prec = 90
        v = (Decimal(sqrt_price_x96) / Decimal(2**96))**2 * (Decimal(10) ** (dec0 - dec1))
        return str(v.normalize())


def quote_calldata(protocol: str, pool: str, token_in: str, token_out: str,
                   amount_in: int, meta_json: str) -> tuple[str, str, str]:
    if amount_in <= 0:
        raise DataError('Quote input must be positive')
    if protocol == 'uniswap_v3':
        fee = int(json.loads(meta_json)['fee'])
        if not 0 <= fee < 2**24:
            raise DataError('Bad pool fee')
        return QUOTER_V2, abi_data('quoteExactInputSingle((address,address,uint256,uint24,uint160))',
                                  token_in, token_out, amount_in, fee, 0), 'uniswap_v3_quoter_v2'
    if protocol == 'aerodrome_classic':
        return address(pool), abi_data('getAmountOut(uint256,address)', amount_in, token_in), 'aerodrome_pool_getAmountOut'
    raise DataError('Unrecognized protocol')


def quote_suspended(c: sqlite3.Connection) -> bool:
    return bool(c.execute('SELECT quotes_suspended FROM rpc_state WHERE id=1').fetchone()[0])


def eth_call_budget(c: sqlite3.Connection, a: argparse.Namespace) -> bool:
    used = c.execute('SELECT COUNT(*) FROM requests WHERE source="rpc" AND method="eth_call" AND started_ms>?',
                     (now_ms()-60_000,)).fetchone()[0]
    return used < a.eth_call_per_minute


def ethcall(c: sqlite3.Connection, t: Transport, target: str, data: str,
            state_block: int, label: str, a: argparse.Namespace):
    if quote_suspended(c):
        raise DataError('Quote subsystem suspended by kill switch')
    if not eth_call_budget(c, a):
        raise DataError('eth_call budget exhausted')
    tag = hex(state_block)
    try:
        # ONLY this RPC call; the QuoterV2 Solidity method is non-view but is never sent as a tx.
        raw, received, rid = call(c, t, 'rpc', 'eth_call',
                                  [{'to': address(target), 'data': data}, tag], pool=label)
    except (TransportError, DataError) as e:
        with c:
            c.execute('UPDATE rpc_state SET call_fail_streak=call_fail_streak+1,call_attempts=call_attempts+1 WHERE id=1')
            streak = c.execute('SELECT call_fail_streak FROM rpc_state WHERE id=1').fetchone()[0]
            if streak >= 5:
                c.execute('UPDATE rpc_state SET quotes_suspended=1 WHERE id=1')
                c.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                          (now_ms(),'quote_kill_switch','five_consecutive_eth_call_failures'))
        raise
    with c:
        c.execute('UPDATE rpc_state SET call_fail_streak=0,call_attempts=call_attempts+1 WHERE id=1')
    return raw, received, rid


def metadata_for(c: sqlite3.Connection, token: str) -> sqlite3.Row | None:
    return c.execute('SELECT * FROM token_metadata WHERE token=?', (token,)).fetchone()


def fetch_one_metadata(c: sqlite3.Connection, t: Transport, a: argparse.Namespace, state_block: int) -> int:
    if not eth_call_budget(c,a):
        return 0
    row = c.execute('''SELECT m.* FROM token_metadata m WHERE
         (m.status="pending" OR (m.status="retry" AND m.attempts<2)) AND m.next_due_ms<=?
         ORDER BY m.next_due_ms,m.token LIMIT 1''', (now_ms(),)).fetchone()
    if row is None:
        return 0
    status, dec, err, rid = 'retry', None, None, None
    try:
        raw, received, rid = ethcall(c,t,row['token'],selector('decimals()'),state_block,
                                    'metadata:'+row['token'],a)
        dec = abi_output(raw, 1)[0]
        if not 0<=dec<=36:
            raise DataError('Token decimals outside 0..36')
        status = 'ok'
    except (TransportError, DataError) as ex:
        received = now_ms()
        err = ex.kind if isinstance(ex,TransportError) else type(ex).__name__
        status = 'failed' if row['attempts'] + 1 >= 2 or quote_suspended(c) else 'retry'
    with c:
        c.execute('''UPDATE token_metadata SET attempts=attempts+1,status=?,decimals=?,error_kind=?,
                    request_id=?,observed_ms=?,state_block=?,next_due_ms=? WHERE token=?''',
                  (status,dec,err,rid,received,state_block,received+30_000,row['token']))
    return 1


def initialize_quote_schedule(c: sqlite3.Connection) -> None:
    """At-least-once idempotent schedule from committed first Swap observation, not block time."""
    pairs = list(c.execute('''SELECT h.pool,h.cohort,h.anchor,h.target,MIN(e.observed_ms) AS observed
         FROM pool_cohorts h JOIN events e ON e.pool=h.pool AND e.within_watch=1
         WHERE h.quote_eligible=1 AND e.event_kind IN ('v3_swap','aero_swap')
         GROUP BY h.pool'''))
    with c:
        for p in pairs:
            if c.execute('SELECT first_swap_seen_ms FROM pool_cohorts WHERE pool=?', (p['pool'],)).fetchone()[0] is not None:
                continue
            seen = p['observed']
            c.execute('UPDATE pool_cohorts SET first_swap_seen_ms=? WHERE pool=?',(seen,p['pool']))
            for token in (p['anchor'],p['target']):
                c.execute('INSERT OR IGNORE INTO token_metadata(token,next_due_ms) VALUES(?,?)', (token,seen))
            size_unit = 'usdc' if p['anchor']==USDC else 'weth'
            for size in SIZES[size_unit]:
                for delay in BUY_DELAYS:
                    c.execute('''INSERT OR IGNORE INTO quote_schedule
                      (pool,side,size_key,buy_delay_s,horizon_min,due_ms,created_ms,status)
                      VALUES(?,?,?,?,?,?,?,?)''',
                      (p['pool'],'buy',size,delay,0,seen+delay*1000,now_ms(),'pending'))
                for horizon in SELL_HORIZONS:
                    c.execute('''INSERT OR IGNORE INTO quote_schedule
                      (pool,side,size_key,buy_delay_s,horizon_min,due_ms,created_ms,status)
                      VALUES(?,?,?,?,?,?,?,?)''',
                      (p['pool'],'sell',size,15,horizon,None,now_ms(),'waiting_buy'))


def cancel_dependent_sells(c: sqlite3.Connection, buyjob: sqlite3.Row, reason: str) -> None:
    if buyjob['side']!='buy' or buyjob['buy_delay_s']!=15:
        return
    c.execute('''UPDATE quote_schedule SET status="censored",reason=?
        WHERE pool=? AND side="sell" AND size_key=? AND status="waiting_buy"''',
        ('no_valid_buy_'+reason,buyjob['pool'],buyjob['size_key']))


def ready_metadata(c: sqlite3.Connection, p: sqlite3.Row) -> tuple[int,int] | None:
    d0=metadata_for(c,p['anchor']); d1=metadata_for(c,p['target'])
    if d0 is None or d1 is None or d0['status']!='ok' or d1['status']!='ok':
        return None
    return d0['decimals'],d1['decimals']


def finalize_quote_jobs(c: sqlite3.Connection, reason: str) -> None:
    with c:
        c.execute('UPDATE quote_schedule SET status="censored",reason=? WHERE status IN ("pending","waiting_buy","in_progress")',
                  (reason,))


def settle_failed_job(c: sqlite3.Connection, job: sqlite3.Row, reason: str,
                      started: int | None=None, received: int | None=None,
                      request_id: int | None=None) -> None:
    with c:
        c.execute('''UPDATE quote_schedule SET status="failed",reason=?,request_started_ms=?,
                     response_received_ms=?,request_id=COALESCE(?,request_id) WHERE id=?''',
                  (reason,started,received or now_ms(),request_id,job['id']))
        cancel_dependent_sells(c,job,reason)


def process_quote_jobs(c: sqlite3.Connection, t: Transport, a: argparse.Namespace, head: int) -> int:
    """Global 6 RPM budget for ALL eth_calls, first discovery then due SELL/BUY then metadata."""
    if a.eth_call_per_minute == 0 or quote_suspended(c):
        return 0
    used=0;state_block=max(0,head-a.confirmations)
    while used < a.max_quote_calls_per_cycle and eth_call_budget(c,a) and not quote_suspended(c):
        moment=now_ms()
        # A pending task late beyond the predeclared budget remains in denominator as censored.
        with c:
            late=list(c.execute('''SELECT * FROM quote_schedule
                WHERE status="pending" AND due_ms+?<?''',
                (a.max_quote_lateness_seconds*1000,moment)))
            for j in late:
                c.execute('UPDATE quote_schedule SET status="censored",reason="scheduler_late" WHERE id=?',(j['id'],))
                cancel_dependent_sells(c,j,'scheduler_late')
        # Prioritize SELL due; then BUY+15 and BUY+60. Dates never shifted to manufacture punctuality.
        job=c.execute('''SELECT s.*,p.dex,p.meta_json,p.token0,p.token1,h.anchor,h.target,
                h.first_swap_seen_ms FROM quote_schedule s
                JOIN pools p ON p.pool=s.pool JOIN pool_cohorts h ON h.pool=s.pool
                WHERE s.status="pending" AND s.due_ms<=?
                ORDER BY CASE WHEN s.side="sell" THEN 0 WHEN s.buy_delay_s=15 THEN 1 ELSE 2 END,
                         s.due_ms,s.id LIMIT 1''',(moment,)).fetchone()
        if job is None:
            n=fetch_one_metadata(c,t,a,state_block)
            used+=n
            if not n:break
            continue
        md=ready_metadata(c,job)
        if md is None:
            failed=[metadata_for(c,z) for z in (job['anchor'],job['target'])]
            if any(r is not None and r['status']=='failed' for r in failed):
                with c:
                    c.execute('UPDATE quote_schedule SET status="censored",reason="metadata_unavailable" WHERE id=?',(job['id'],))
                    cancel_dependent_sells(c,job,'metadata_unavailable')
                continue
            n=fetch_one_metadata(c,t,a,state_block)
            used+=n
            if not n:break
            continue
        anchor_dec, target_dec = md
        if job['side']=='buy':
            raw_in=raw_for_size(job['anchor'],job['size_key'],anchor_dec)
            token_in,token_out=job['anchor'],job['target']
        else:
            ref=c.execute('''SELECT q.amount_out_raw FROM quotes q
                JOIN quote_schedule s ON s.id=q.job_id WHERE s.id=?''',(job['buy_job_id'],)).fetchone()
            if not ref:
                settle_failed_job(c,job,'buy_reference_missing')
                continue
            raw_in=int(ref['amount_out_raw'])
            token_in,token_out=job['target'],job['anchor']
        target,data,label=quote_calldata(job['dex'],job['pool'],token_in,token_out,raw_in,job['meta_json'])
        reqstart=now_ms()
        with c:
            c.execute('UPDATE quote_schedule SET status="in_progress",request_started_ms=?,state_block=?,head_at_request=? WHERE id=?',
                      (reqstart,state_block,head,job['id']))
        rid = None
        try:
            response,received,rid=ethcall(c,t,target,data,state_block,'quote:'+job['pool'],a)
            with c:
                c.execute('UPDATE quote_schedule SET request_id=? WHERE id=?', (rid,job['id']))
            parsed=parse_v3_quote(response) if job['dex']=='uniswap_v3' else parse_aero_quote(response)
            # Keep the block tag and its hash; verify block state is not relabeled historical.
            state_ms, state_hash=block_info(c,t,state_block,{})
            if state_ms>received+3_000:
                raise DataError('Quote state is from future block time')
        except (TransportError,DataError,ValueError,sqlite3.Error) as ex:
            failed_id = ex.request_id if isinstance(ex,TransportError) else rid
            settle_failed_job(c,job,ex.kind if isinstance(ex,TransportError) else type(ex).__name__,
                              reqstart,now_ms(),failed_id)
            used+=1
            continue
        with c:
            c.execute('''INSERT INTO quotes
              (job_id,pool,side,anchor,target,amount_in_raw,amount_out_raw,
               observed_ms,request_id,state_block,state_hash,state_time_ms,head_at_request,
               quoter,sqrt_price_after,ticks_crossed,quoter_gas_estimate)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (job['id'],job['pool'],job['side'],job['anchor'],job['target'],str(raw_in),
               str(parsed['amount_out']),received,rid,state_block,state_hash,state_ms,head,label,
               str(parsed['sqrt_after']) if parsed['sqrt_after'] is not None else None,
               parsed['ticks'],str(parsed['quoter_gas']) if parsed['quoter_gas'] is not None else None))
            c.execute('UPDATE quote_schedule SET status="completed",response_received_ms=? WHERE id=?',(received,job['id']))
            if job['side']=='buy' and job['buy_delay_s']==15:
                c.execute('''INSERT OR IGNORE INTO pool_state(pool,status,protocol,first_started_ms)
                    VALUES(?,?,?,?)''',(job['pool'],'pending',job['dex'],received))
                for h in SELL_HORIZONS:
                    c.execute('''UPDATE quote_schedule SET status="pending",due_ms=?,buy_job_id=?
                        WHERE pool=? AND side="sell" AND size_key=? AND horizon_min=? AND status="waiting_buy"''',
                        (received+h*60_000,job['id'],job['pool'],job['size_key'],h))
        used+=1
    while used < a.max_quote_calls_per_cycle and eth_call_budget(c,a) and not quote_suspended(c):
        n=sample_one_pool_state(c,t,a,head)
        if not n:break
        used+=n
    return used


def sample_one_pool_state(c: sqlite3.Connection, t: Transport, a: argparse.Namespace, head: int) -> int:
    """Low-priority contextual state sampling; never substitute slot0 for a fill quote."""
    row=c.execute('''SELECT st.*,p.token0,p.token1 FROM pool_state st JOIN pools p ON p.pool=st.pool
             WHERE st.status IN ('pending','partial') ORDER BY st.first_started_ms LIMIT 1''').fetchone()
    if row is None:return 0
    state_block=row['state_block'] if row['state_block'] is not None else max(0,head-a.confirmations)
    try:
        if row['protocol']=='uniswap_v3' and row['status']=='pending':
            res,ms,_=ethcall(c,t,row['pool'],selector('slot0()'),state_block,'pool_slot0',a)
            values=abi_output(res,7)
            sqrt=values[0]
            m0=metadata_for(c,row['token0']);m1=metadata_for(c,row['token1'])
            if not (m0 and m1 and m0['status']=='ok' and m1['status']=='ok'):
                raise DataError('Cannot normalize pool price without both decimals')
            price=price_token1_per_token0(sqrt,m0['decimals'],m1['decimals'])
            with c:
                c.execute('''UPDATE pool_state SET status='partial',state_block=?,last_observed_ms=?,
                     sqrt_price_x96=?,spot_token1_per_token0=? WHERE pool=?''',
                     (state_block,ms,str(sqrt),price,row['pool']))
            return 1
        elif row['protocol']=='uniswap_v3':
            res,ms,_=ethcall(c,t,row['pool'],selector('liquidity()'),state_block,'pool_liquidity',a)
            liq=abi_output(res,1)[0]
            state_time,bhash=block_info(c,t,state_block,{})
            with c:
                c.execute('''UPDATE pool_state SET status='complete',state_hash=?,last_observed_ms=?,
                     active_liquidity_raw=? WHERE pool=?''',(bhash,ms,str(liq),row['pool']))
            return 1
        elif row['protocol']=='aerodrome_classic':
            res,ms,_=ethcall(c,t,row['pool'],selector('getReserves()'),state_block,'pool_reserves',a)
            r0,r1,_=abi_output(res,3)
            state_time,bhash=block_info(c,t,state_block,{})
            with c:
                c.execute('''UPDATE pool_state SET status='complete',state_block=?,state_hash=?,
                     last_observed_ms=?,reserve0_raw=?,reserve1_raw=? WHERE pool=?''',
                     (state_block,bhash,ms,str(r0),str(r1),row['pool']))
            return 1
        raise DataError('Unrecognized pool state protocol')
    except (TransportError,DataError,ValueError,sqlite3.Error) as ex:
        with c:
            c.execute('UPDATE pool_state SET status="failed",error_kind=? WHERE pool=?',
                      (ex.kind if isinstance(ex,TransportError) else type(ex).__name__,row['pool']))
        return 1


def audit_quotes(c: sqlite3.Connection, pool_filter: str | None = None) -> dict:
    '''Fully local READ ONLY chronological audit. Also supports legacy 0.3a DBs.

    Event inclusion is limited to the original prospective watch; no claim that
    we know events after monitoring ended, and no historical RPC lookups.
    '''
    hdr = c.execute('SELECT schema_v,version,phase,started_ms,completed_ms FROM campaign WHERE id=1').fetchone()
    if hdr is None or (hdr['schema_v'],hdr['version']) not in ((3,'0.3a'), (4,'0.3a.1')):
        raise DataError('Audit requires a V0.3a or V0.3a.1 database')
    has_link = hdr['schema_v'] >= 4
    result = {'audit_version': '0.3a.1', 'source_version': hdr['version'],
              'campaign_phase': hdr['phase'],
              'read_only': True, 'onchain_replay': False,
              'legacy_failed_rpc_linkage_unavailable': not has_link,
              'limitations': [
                  'Event order by block and log_index is canonical; VPS observed_ms is response time, not event execution time',
                  'Quote state_block is an eth_call state reference, not a trade or a filled quote',
                  'Events are limited to the recorded followup; none after end is not evidence of none on-chain',
                  'No price/impact, real sellability, or PnL inferred from these diagnostics',
              ],'pools':[]}
    if pool_filter is not None:
        pool_filter = address(pool_filter)
        if c.execute('SELECT 1 FROM pools WHERE pool=?',(pool_filter,)).fetchone() is None:
            raise DataError('Requested audit pool is not in campaign')
        pool_list = [pool_filter]
    else:
        pool_list = [r[0] for r in c.execute('''SELECT DISTINCT pool FROM quote_schedule
                  WHERE status IN ('failed','censored') ORDER BY pool''')]
    for pool in pool_list:
        evs = list(c.execute('''SELECT event_kind,block_number,log_index,observed_ms,block_time_ms,
                              tx_hash,within_watch FROM events WHERE pool=?
                              ORDER BY block_number,log_index''',(pool,)))
        tasks = list(c.execute('''SELECT id,side,size_key,buy_delay_s,horizon_min,
                         due_ms,status,reason,request_started_ms,response_received_ms,
                         state_block,head_at_request
                         ''' + (',request_id' if has_link else '') + '''
                         FROM quote_schedule WHERE pool=? AND status IN ('failed','censored')
                         ORDER BY COALESCE(request_started_ms,due_ms),id''',(pool,)))
        all_status = {k:n for k,n in c.execute('''SELECT status,COUNT(*) FROM quote_schedule
                          WHERE pool=? GROUP BY status''',(pool,))}
        p = {'pool':pool, 'job_status':all_status,
             'recorded_event_counts':{k:n for k,n in c.execute('''SELECT event_kind,COUNT(*)
                                      FROM events WHERE pool=? GROUP BY event_kind''',(pool,))},
             'problem_jobs': [], 'observed_timeline': []}
        # Include success and failure jobs so the reader can establish the full
        # observed sequence. Timestamp is VPS observation / request START,
        # not block_time or an assumed historical execution timestamp.
        timeline=[]
        for ev in evs:
            if ev['event_kind'].endswith(('_mint','_burn','_swap')):
                timeline.append({'kind':'onchain_event','observed_ms':ev['observed_ms'],
                    'event_kind':ev['event_kind'],'event_block':ev['block_number'],
                    'log_index':ev['log_index'],'within_watch':bool(ev['within_watch'])})
        all_jobs = c.execute('''SELECT id,side,size_key,buy_delay_s,horizon_min,
                            due_ms,status,reason,request_started_ms,response_received_ms,state_block
                            FROM quote_schedule WHERE pool=?''',(pool,))
        for j in all_jobs:
            timeline.append({'kind':'quote_job','observed_ms':j['request_started_ms'],
                'due_ms':j['due_ms'],'job_id':j['id'],'side':j['side'],
                'size_key':j['size_key'],'buy_delay_s':j['buy_delay_s'],
                'horizon_min':j['horizon_min'],'state_block':j['state_block'],
                'status':j['status'],'reason':j['reason'],
                'response_received_ms':j['response_received_ms']})
        p['observed_timeline'] = sorted(timeline,
            key=lambda j: (j['observed_ms'] is None,
                j['observed_ms'] if j['observed_ms'] is not None else (j.get('due_ms') or 0),
                0 if j['kind']=='onchain_event' else 1))
        for job in tasks:
            state = job['state_block']
            started = job['request_started_ms']
            # Observed-time order and chain block order are distinct axes.
            def event_minimal(ev):
                return {'kind':ev['event_kind'],'block':ev['block_number'],
                        'log_index':ev['log_index'],'block_time_ms':ev['block_time_ms'],
                        'observed_ms':ev['observed_ms'],'tx_hash':ev['tx_hash'],
                        'within_watch':bool(ev['within_watch'])}
            kind_counts = {}
            for typ in ('mint','burn','swap'):
                matches = [ev for ev in evs if ev['event_kind'].endswith('_'+typ)]
                kind_counts[typ] = {
                    'recorded_total':len(matches),
                    'state_block_at_or_before':sum(ev['block_number']<=state for ev in matches) if state is not None else None,
                    'observed_before_request':sum(ev['observed_ms']<=started for ev in matches) if started is not None else None,
                }
            before = [] if started is None else sorted(
                (ev for ev in evs if ev['observed_ms']<=started),
                key=lambda e:(e['observed_ms'],e['block_number'],e['log_index']), reverse=True)[:3]
            after = [] if started is None else sorted(
                (ev for ev in evs if ev['observed_ms']>started),
                key=lambda e:(e['observed_ms'],e['block_number'],e['log_index']))[:3]
            req = None
            if has_link and job['request_id'] is not None:
                rr = c.execute('''SELECT id,source,method,started_ms,ended_ms,success,
                          http_status,error_kind,error_diag_json FROM requests WHERE id=?''',
                          (job['request_id'],)).fetchone()
                if rr:
                    req = {k:rr[k] for k in ('id','source','method','started_ms','ended_ms','success','http_status','error_kind')}
                    req['safe_diagnostic'] = json.loads(rr['error_diag_json']) if rr['error_diag_json'] else None
            p['problem_jobs'].append({
                'job_id':job['id'],'side':job['side'],'size_key':job['size_key'],
                'buy_delay_s':job['buy_delay_s'],'horizon_min':job['horizon_min'],
                'status':job['status'],'reason':job['reason'],
                'due_ms':job['due_ms'],'request_started_ms':started,
                'response_received_ms':job['response_received_ms'],
                'state_block':state,'head_at_request':job['head_at_request'],
                'linked_rpc_request':req,
                'mint_burn_swap_context':kind_counts,
                'nearest_events_observed_before_request':[event_minimal(ev) for ev in before],
                'nearest_events_observed_after_request':[event_minimal(ev) for ev in after],
            })
        result['pools'].append(p)
    result['problem_pools_count']=len(result['pools'])
    result['problem_jobs_count']=sum(len(p['problem_jobs']) for p in result['pools'])
    return result


def quote_report(c: sqlite3.Connection) -> dict:
    result={'cohort_counts':{},'reasons':{},'direct_anchor_eligible':0,'first_swap_observed':0,
            'metadata_status':{},'job_status':{},'success_by_side':{},'pool_state_samples':{},'eth_call_requests':0,
            'eth_call_errors':0,'quotes_suspended':quote_suspended(c),
            'buy_120s_covered_pools':0,'sell_6min_covered_pools':0,
            'buy_delay_15_s':{},'buy_delay_60_s':{},'all_missing_are_censored':True,
            'interpretation':'Technical quotes, NOT executed swaps, sellability guarantee or PnL'}
    for k,n in c.execute('SELECT cohort,COUNT(*) FROM pool_cohorts GROUP BY cohort'):result['cohort_counts'][k]=n
    for k,n in c.execute('SELECT reason,COUNT(*) FROM pool_cohorts GROUP BY reason'):result['reasons'][k]=n
    for k,n in c.execute('SELECT status,COUNT(*) FROM token_metadata GROUP BY status'):result['metadata_status'][k]=n
    for k,n in c.execute('SELECT status,COUNT(*) FROM quote_schedule GROUP BY status'):result['job_status'][k]=n
    for k,n in c.execute('SELECT status,COUNT(*) FROM pool_state GROUP BY status'):result['pool_state_samples'][k]=n
    for k,n in c.execute('SELECT side,COUNT(*) FROM quotes GROUP BY side'):result['success_by_side'][k]=n
    # Diagnostics are observational, not economic performance metrics.
    result['diagnostic_linkage'] = {
        'completed_jobs_with_request_id':c.execute('''SELECT COUNT(*) FROM quote_schedule
              WHERE status='completed' AND request_id IS NOT NULL''').fetchone()[0],
        'failed_jobs_with_request_id':c.execute('''SELECT COUNT(*) FROM quote_schedule
              WHERE status='failed' AND request_id IS NOT NULL''').fetchone()[0],
        'failed_jobs_without_request_id':c.execute('''SELECT COUNT(*) FROM quote_schedule
              WHERE status='failed' AND request_id IS NULL''').fetchone()[0],
        'censored_jobs':c.execute('''SELECT COUNT(*) FROM quote_schedule
              WHERE status='censored' ''').fetchone()[0],
    }
    result['rpc_eth_call_diagnostics'] = {}
    for raw,n in c.execute('''SELECT error_diag_json,COUNT(*) FROM requests
               WHERE source='rpc' AND method='eth_call' AND success=0 GROUP BY error_diag_json'''):
        if raw:
            obj=json.loads(raw)
            label=':'.join((obj.get('message_class','unknown'),obj.get('selector','no_selector'),
                            obj.get('allowlisted_reason','unknown_reason')))
        else:label='no_structured_diagnostic'
        result['rpc_eth_call_diagnostics'][label]=result['rpc_eth_call_diagnostics'].get(label,0)+n
    result['direct_anchor_eligible']=c.execute('SELECT COUNT(*) FROM pool_cohorts WHERE quote_eligible=1').fetchone()[0]
    result['first_swap_observed']=c.execute('SELECT COUNT(*) FROM pool_cohorts WHERE quote_eligible=1 AND first_swap_seen_ms IS NOT NULL').fetchone()[0]
    rr=c.execute('''SELECT COUNT(*),COALESCE(SUM(success=0),0) FROM requests
                    WHERE source='rpc' AND method='eth_call' ''').fetchone()
    result['eth_call_requests'],result['eth_call_errors']=rr
    result['buy_120s_covered_pools']=c.execute('''SELECT COUNT(DISTINCT s.pool) FROM quote_schedule s
         JOIN quotes q ON q.job_id=s.id JOIN pool_cohorts h ON h.pool=s.pool
         WHERE s.side='buy' AND q.observed_ms-h.first_swap_seen_ms<=120000''').fetchone()[0]
    result['sell_6min_covered_pools']=c.execute('''SELECT COUNT(DISTINCT s.pool) FROM quote_schedule s
         JOIN quotes q ON q.job_id=s.id JOIN quotes b ON b.job_id=s.buy_job_id
         WHERE s.side='sell' AND s.horizon_min=5 AND q.observed_ms-b.observed_ms<=360000''').fetchone()[0]
    for delay in BUY_DELAYS:
        key='buy_delay_'+str(delay)+'_s'
        rows=list(c.execute('''SELECT q.observed_ms,s.due_ms,h.first_swap_seen_ms
          FROM quote_schedule s JOIN quotes q ON q.job_id=s.id JOIN pool_cohorts h ON h.pool=s.pool
          WHERE s.side='buy' AND s.buy_delay_s=?''',(delay,)))
        result[key]={'successful_quotes':len(rows),'scheduled_jobs':c.execute(
            'SELECT COUNT(*) FROM quote_schedule WHERE side="buy" AND buy_delay_s=?',(delay,)).fetchone()[0],
            'due_to_observation_p50_s':pct([(r['observed_ms']-r['due_ms'])/1000 for r in rows],50),
            'due_to_observation_p95_s':pct([(r['observed_ms']-r['due_ms'])/1000 for r in rows],95)}
    return result


def wait_for_smoke_budget(c: sqlite3.Connection, a: argparse.Namespace) -> None:
    """Bounded wait, never bypass the configured 60-second eth_call budget."""
    if a.eth_call_per_minute == 0:
        raise DataError('Contract smoke requires positive eth_call budget')
    while not eth_call_budget(c,a):
        old=c.execute('''SELECT MIN(started_ms) FROM requests WHERE source='rpc'
                    AND method='eth_call' AND started_ms>?''',(now_ms()-60_000,)).fetchone()[0]
        if old is None:break
        waiting=min(61.0,max(1.0,(old+60_010-now_ms())/1000))
        print(f'CONTRACT SMOKE: waiting {waiting:.1f}s to respect eth_call budget',flush=True)
        time.sleep(waiting)


def contract_smoke(a: argparse.Namespace) -> int:
    """Independent, one-shot known WETH/USDC pool check, persisted in fresh DB.

    Historical known liquid anchor pool is for ABI/transport validation ONLY,
    never a retrospective momentum signal and never mixed into prospective cohorts.
    """
    transport=Transport(a.rpc_url,a.http_timeout)
    path=Path(a.db)
    if path.exists():raise DataError('Contract smoke requires a new DB')
    identity,_=transport.rpc('eth_chainId',[])
    if int(identity,16)!=CHAIN:raise DataError('Contract smoke endpoint is not Base')
    head_raw,_=transport.rpc('eth_blockNumber',[])
    head=int(head_raw,16)
    c=create_db(path,a,head,now_ms())
    checks=[]
    try:
        state_block=max(0,head-a.confirmations)
        fees=(500,3000,10000)
        chosen=None
        for fee in fees:
            wait_for_smoke_budget(c,a)
            out,_,_=ethcall(c,transport,UNI_FACTORY,
                 abi_data('getPool(address,address,uint24)',WETH,USDC,fee),state_block,'smoke_factory',a)
            pool_num=abi_output(out,1)[0]
            if pool_num > 0 and pool_num < 2**160:
                chosen=('0x'+f'{pool_num:040x}',fee)
                break
        if chosen is None:
            raise DataError('No factory WETH/USDC pool found within smoke RPC budget')
        pool,fee=chosen
        code,_,_=call(c,transport,'rpc','eth_getCode',[pool,hex(state_block)],pool='smoke_code')
        if not isinstance(code,str) or len(code)<=2:
            raise DataError('Known pool has no code')
        # Six eth_calls/min applies also to contract smoke; budget exhaustion is
        # reported as incomplete rather than ignored or bypassed.
        for token in (WETH,USDC):
            wait_for_smoke_budget(c,a)
            res,_,_=ethcall(c,transport,token,selector('decimals()'),state_block,'smoke_decimals',a)
            dec=abi_output(res,1)[0]
            if (token==WETH and dec!=18) or (token==USDC and dec!=6):
                raise DataError('Reference token decimals mismatch')
            checks.append(('decimals',token,dec))
        wait_for_smoke_budget(c,a)
        out,_,_=ethcall(c,transport,pool,selector('slot0()'),state_block,'smoke_slot0',a)
        slot=abi_output(out,7)
        if not 0 < slot[0] < 2**160:
            raise DataError('Invalid pool slot0 sqrt price')
        wait_for_smoke_budget(c,a)
        liqraw,_,_=ethcall(c,transport,pool,selector('liquidity()'),state_block,'smoke_liquidity',a)
        liquidity=abi_output(liqraw,1)[0]
        wait_for_smoke_budget(c,a)
        _,payload,_=quote_calldata('uniswap_v3',pool,USDC,WETH,10*10**6,jdump({'fee':fee}))
        qraw,seen,rid=ethcall(c,transport,QUOTER_V2,payload,state_block,'smoke_quoter',a)
        q=parse_v3_quote(qraw)
        checks.extend([('pool',pool,fee),('slot0_sqrt',pool,str(slot[0])),('liquidity_raw',pool,str(liquidity)),
                       ('quote_10_usdc_to_weth_raw',pool,str(q['amount_out']))])
        with c:
            c.execute('UPDATE campaign SET phase="complete",completed_ms=? WHERE id=1',(now_ms(),))
        print('CONTRACT SMOKE OK (READ ONLY; not a trade):',jdump({'block':state_block,
                'pool':pool,'fee':fee,'checks':checks,'response_ms':seen}),flush=True)
        return 0
    except (DataError,TransportError,sqlite3.Error) as exc:
        kind=exc.kind if isinstance(exc,TransportError) else type(exc).__name__
        with c:
            c.execute('UPDATE campaign SET phase="incomplete_backlog",completed_ms=? WHERE id=1',(now_ms(),))
            c.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                      (now_ms(),'contract_smoke_failed',kind))
        print('CONTRACT SMOKE FAILED: '+kind+'; DB preserved; no trading',flush=True)
        return 2
    finally:
        c.close()


class Tests(unittest.TestCase):
    def args(self,**kwargs):
        a=parse_args(['--db','unused.sqlite3'])
        for k,v in kwargs.items():setattr(a,k,v)
        return a
    @staticmethod
    def w(n):return '0x'+f'{n%(1<<256):064x}'
    @staticmethod
    def pack(*xs):return '0x'+''.join(f'{v%(1<<256):064x}' for v in xs)
    def creation(self,aero=False,block=101,idx=0,pool=999):
        return dict(address=AERO_FACTORY if aero else UNI_FACTORY,
          topics=[FACTORY_TOPICS['aerodrome_classic' if aero else 'uniswap_v3'],
                  self.w(111),self.w(222),self.w(1 if aero else 3000)],
          data=self.pack(pool,567) if aero else self.pack(60,pool),
          blockNumber=hex(block),logIndex=hex(idx),transactionHash=self.w(321),
          blockHash=self.w(555),removed=False)
    def event(self,kind,block=101,idx=1,pool=999):
        base=dict(address='0x'+f'{pool:040x}',topics=[EVT_TOPICS[kind]],data='0x',
           blockNumber=hex(block),logIndex=hex(idx),transactionHash=self.w(321),
           blockHash=self.w(555),removed=False)
        if kind=='v3_swap':
            base['topics'] += [self.w(10),self.w(11)]
            base['data']=self.pack(100, -50, 2**96,2000,-1)
        elif kind=='v3_mint':
            base['topics'] += [self.w(10),self.w(-100),self.w(100)]
            base['data']=self.pack(12,300,1000,2000)
        elif kind=='v3_burn':
            base['topics'] += [self.w(10),self.w(-100),self.w(100)]
            base['data']=self.pack(300,1000,2000)
        elif kind=='v3_initialize':base['data']=self.pack(2**96,0)
        elif kind=='aero_swap':
            base['topics'] += [self.w(10),self.w(11)]
            base['data']=self.pack(1000,0,0,100)
        elif kind=='aero_mint':
            base['topics'] += [self.w(10)]
            base['data']=self.pack(1000,2000)
        elif kind=='aero_burn':
            base['topics'] += [self.w(10),self.w(11)]
            base['data']=self.pack(1000,2000)
        elif kind=='aero_sync':base['data']=self.pack(1000,2000)
        return base
    def fixture(self,d,**kw):
        a=self.args(db=str(Path(d)/'research.sqlite3'),**kw)
        c=create_db(Path(a.db),a,100,now_ms());return c,a
    def test_keccak_vectors(self):
        self.assertEqual(keccak256(b'').hex(),'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470')
        self.assertEqual(EVT_TOPICS['v3_mint'],'0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde')
    def test_decode_v3_creation(self):
        x=decode_creation(self.creation());self.assertEqual(x['dex'],'uniswap_v3')
        self.assertEqual(json.loads(x['meta'])['tick_spacing'],60)
    def test_decode_aero_creation(self):
        x=decode_creation(self.creation(aero=True));self.assertEqual(x['dex'],'aerodrome_classic')
    def test_v3_swap_raw_sign_and_direction(self):
        x=decode_event(self.event('v3_swap'),'uniswap_v3','0x'+f'{999:040x}')
        raw=json.loads(x['raw']);self.assertEqual(raw['amount1_delta'],'-50')
        self.assertEqual(raw['direction_token0'],'token0_in')
        self.assertNotIn('price_usd',raw)
    def test_v3_mint_burn_init(self):
        for name in ('v3_initialize','v3_mint','v3_burn'):
            q=decode_event(self.event(name),'uniswap_v3','0x'+f'{999:040x}')
            self.assertEqual(q['kind'],name)
    def test_aero_events(self):
        for name in ('aero_sync','aero_swap','aero_mint','aero_burn'):
            q=decode_event(self.event(name),'aerodrome_classic','0x'+f'{999:040x}')
            self.assertEqual(q['kind'],name)
    def test_aero_swap_direction(self):
        q=decode_event(self.event('aero_swap'),'aerodrome_classic','0x'+f'{999:040x}')
        self.assertEqual(json.loads(q['raw'])['direction_token0'],'token0_in')
    def test_no_cross_protocol(self):
        with self.assertRaises(DataError):decode_event(self.event('v3_swap'),'aerodrome_classic','0x'+f'{999:040x}')
    def test_wrong_pool(self):
        with self.assertRaises(DataError):decode_event(self.event('v3_swap'),'uniswap_v3','0x'+'a'*40)
    def test_malformed_topic(self):
        e=self.event('v3_swap');e['topics']=[EVT_TOPICS['v3_swap']]
        with self.assertRaises(DataError):decode_event(e,'uniswap_v3','0x'+f'{999:040x}')
    def test_corrupt_abi(self):
        e=self.event('v3_swap');e['data']='0x10'
        with self.assertRaises(DataError):decode_event(e,'uniswap_v3','0x'+f'{999:040x}')
    def test_removed_log(self):
        e=self.event('v3_swap');e['removed']=True
        with self.assertRaises(DataError):decode_event(e,'uniswap_v3','0x'+f'{999:040x}')
    def test_rpc_whitelist(self):
        t=Transport(DEFAULT_RPC,5)
        with self.assertRaises(DataError):t.rpc('eth_sendRawTransaction',[])
    def test_http_remote_disallowed(self):
        with self.assertRaises(DataError):Transport('http://example.com',5)
    def test_pool_admission_and_event_same_block(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            pool=decode_creation(self.creation());pool['block_time_ms']=now_ms()-10_000
            ev=decode_event(self.event('v3_swap'),'uniswap_v3',pool['pool']);ev['block_time_ms']=pool['block_time_ms']
            n,k,m=persist_range(c,[pool],[ev],101,101,now_ms(),a)
            self.assertEqual((n,k,m),(1,0,1))
            self.assertEqual(report(c)['event_counts_within_watch']['v3_swap'],1)
            self.assertEqual(campaign(c)['next_block'],102)
            c.close()
    def test_empty_range_is_valid(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            persist_range(c,[],[],101,105,now_ms(),a)
            self.assertEqual(report(c)['scanned_blocks'],5)
            c.close()
    def test_gap_rejected_no_cursor_advance(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            with self.assertRaises(DataError):persist_range(c,[],[],102,103,now_ms(),a)
            self.assertEqual(campaign(c)['next_block'],101)
            c.close()
    def test_capacity_exclusions_are_preserved(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d,max_active_pools=1,max_pool_addresses_per_query=2)
            p=decode_creation(self.creation());p['block_time_ms']=now_ms()-5000
            q=decode_creation(self.creation(aero=True,pool=998,idx=1));q['block_time_ms']=p['block_time_ms']
            n,k,m=persist_range(c,[p,q],[],101,101,now_ms(),a)
            self.assertEqual((n,k),(1,1))
            self.assertEqual(report(c)['admission_initial']['not_admitted_capacity'],1)
            c.close()
    def test_after_deadline_not_prospective(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            c.execute('UPDATE campaign SET phase="draining",cutoff_block=101');c.commit()
            p=decode_creation(self.creation());p['block_time_ms']=now_ms()-10_000
            n,k,m=persist_range(c,[p],[],101,101,now_ms(),a)
            self.assertEqual((n,k),(0,1))
            self.assertEqual(report(c)['admission_initial']['recovered_after_cutoff'],1)
            c.close()
    def test_followup_censored_if_late(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d,watch_minutes=1)
            p=decode_creation(self.creation());p['block_time_ms']=now_ms()-120_000
            persist_range(c,[p],[],101,101,now_ms()-70_000,a)
            e=decode_event(self.event('v3_swap',block=102),'uniswap_v3',p['pool']);e['block_time_ms']=p['block_time_ms']+5000
            persist_range(c,[],[e],102,102,now_ms(),a)
            self.assertEqual(report(c)['event_counts_all']['v3_swap'],1)
            self.assertNotIn('v3_swap',report(c)['event_counts_within_watch'])
            c.close()
    def test_report_read_only(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d);c.close()
            before=Path(a.db).read_bytes()
            ro=connect(Path(a.db),True);report(ro)
            with self.assertRaises(sqlite3.OperationalError):ro.execute('DELETE FROM campaign')
            ro.close();self.assertEqual(before,Path(a.db).read_bytes())
    def test_dex_exact_match(self):
        p='0x'+'a'*40
        self.assertFalse(provider_fields('dex',{'pairs':[{'pairAddress':p,'chainId':'ethereum'}]},p)['listed'])
        self.assertTrue(provider_fields('dex',{'pairs':[{'pairAddress':p,'chainId':'base','priceUsd':'2'}]},p)['priced'])
    def test_gecko_exact_match(self):
        p='0x'+'a'*40
        x={'data':{'id':'base_'+p,'attributes':{'address':p,'base_token_price_usd':'0.2'}}}
        self.assertTrue(provider_fields('gecko',x,p)['priced'])
    def test_missing_quote_is_not_zero(self):
        p='0x'+'a'*40
        x=provider_fields('dex',{'pairs':[{'pairAddress':p,'chainId':'base','priceUsd':None}]},p)
        self.assertTrue(x['listed']);self.assertFalse(x['priced']);self.assertIsNone(x['price_raw'])
    def test_nonexistent_report_refused(self):
        with TemporaryDirectory() as d:
            with self.assertRaises(DataError):connect(Path(d)/'none.db',True)
    def test_end_to_end_mock_chain_and_providers(self):
        with TemporaryDirectory() as d:
            a=self.args(db=str(Path(d)/'full.sqlite3'),once=True)
            creation=self.creation(block=101)
            p='0x'+f'{999:040x}'
            swap=self.event('v3_swap',block=101,idx=2)
            class Fake:
                def rpc(self,method,params):
                    if method=='eth_chainId':return hex(CHAIN),200
                    if method=='eth_blockNumber':return ('0x64' if not hasattr(self,'got_head') else '0x6e'),200
                    if method=='eth_getLogs':
                        filt=params[0]
                        if UNI_FACTORY in filt['address']:
                            return [creation] if int(filt['fromBlock'],16)<=101<=int(filt['toBlock'],16) else [],200
                        return [swap] if int(filt['fromBlock'],16)<=101<=int(filt['toBlock'],16) else [],200
                    if method=='eth_getBlockByNumber':
                        return {'number':hex(101),'timestamp':hex(int(time.time())-5),'hash':self_outer.w(555)},200
                    raise AssertionError(method)
                def request(self,url,gecko=False):
                    if gecko:return {'data':{'id':'base_'+p,'attributes':{'address':p,'base_token_price_usd':'0.2'}}},200
                    return {'pairs':[{'chainId':'base','pairAddress':p,'priceUsd':'0.2'}]},200
            self_outer=self
            f=Fake();f.got_head=True
            # Direct initialize then use fake after tip 110 for events.
            c=create_db(Path(a.db),a,100,now_ms());c.close()
            a.resume=True
            self.assertEqual(run(a,f),0)
            ro=connect(Path(a.db),True)
            res=report(ro)
            self.assertEqual(res['creation_events_by_dex']['uniswap_v3'],1)
            self.assertEqual(res['event_counts_within_watch']['v3_swap'],1)
            self.assertEqual(res['provider']['dex']['first_priced'],1)
            self.assertEqual(res['provider']['gecko']['first_priced'],1)
            self.assertEqual(res['rpc_errors'],0)
            ro.close()
    def test_paired_observation_preserves_same_pool_comparison(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            first=now_ms()
            p=decode_creation(self.creation());p['block_time_ms']=first-10_000;p['observed_ms']=first
            swap=decode_event(self.event('v3_swap'),'uniswap_v3',p['pool'])
            swap['block_time_ms']=first-10_000;swap['observed_ms']=first+1_000
            persist_range(c,[p],[swap],101,101,first,a)
            # The provider first-price timestamp is deliberately later than the swap.
            with c:
                c.execute('UPDATE provider_state SET first_priced_ms=? WHERE pool=? AND source="dex"',
                          (first+6_000,p['pool']))
                c.execute('UPDATE provider_state SET first_priced_ms=? WHERE pool=? AND source="gecko"',
                          (first+500,p['pool']))
            x=report(c)['paired_observation']
            self.assertEqual(x['dex_swap']['onchain_observed_before_provider'],1)
            self.assertEqual(x['dex_swap']['provider_minus_onchain_p50_s'],5.0)
            self.assertEqual(x['gecko_swap']['provider_observed_before_onchain'],1)
            self.assertEqual(x['gecko_swap']['provider_minus_onchain_p50_s'],-0.5)
            c.close()

    def test_paired_observation_keeps_missing_provider_price(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            p=decode_creation(self.creation());p['block_time_ms']=now_ms()-1_000
            s=decode_event(self.event('v3_swap'),'uniswap_v3',p['pool'])
            s['block_time_ms']=p['block_time_ms']
            persist_range(c,[p],[s],101,101,now_ms(),a)
            z=report(c)['paired_observation']['dex_swap']
            self.assertEqual(z['pool_with_observed_event'],1)
            self.assertEqual(z['provider_price_not_observed'],1)
            self.assertEqual(z['both_event_and_provider_price'],0)
            c.close()

    def test_tip_failures_halt_without_advancing_cursor(self):
        with TemporaryDirectory() as d:
            a=self.args(db=str(Path(d)/'tipfail.db'),max_consecutive_scan_errors=2,
                        poll_seconds=2,no_providers=True)
            class FailTip:
                def rpc(self,method,params):
                    if method=='eth_chainId':return hex(CHAIN),200
                    if method=='eth_blockNumber':
                        self.count=getattr(self,'count',0)+1
                        if self.count==1:return '0x64',200
                        raise TransportError('http_429',429)
                    raise AssertionError(method)
            original_sleep=time.sleep
            try:
                time.sleep=lambda _:None
                self.assertEqual(run(a,FailTip()),2)
            finally:
                time.sleep=original_sleep
            ro=connect(Path(a.db),True)
            r=report(ro)
            self.assertEqual(r['campaign']['phase'],'incomplete_backlog')
            self.assertEqual(r['scanned_blocks'],0)
            self.assertEqual(r['issues']['scan_error_tip_http_429'],2)
            self.assertEqual(r['issues']['campaign_halt_max_consecutive_scan_errors'],1)
            ro.close()

    def test_rpc_consecutive_failure_fails_closed(self):
        with TemporaryDirectory() as d:
            a=self.args(db=str(Path(d)/'failure.db'),max_consecutive_scan_errors=2,
                        no_providers=True)
            class FailRPC:
                def rpc(self,method,params):
                    if method=='eth_chainId':return hex(CHAIN),200
                    if method=='eth_blockNumber':
                        self.head_calls=getattr(self,'head_calls',0)+1
                        return ('0x64' if self.head_calls==1 else '0x70'),200
                    if method=='eth_getLogs':raise TransportError('http_429',429)
                    raise AssertionError(method)
            f=FailRPC()
            self.assertEqual(run(a,f),2)
            r=connect(Path(a.db),True)
            rep=report(r)
            self.assertEqual(rep['campaign']['phase'],'incomplete_backlog')
            self.assertEqual(rep['scanned_blocks'],0)
            self.assertEqual(rep['issues']['campaign_halt_max_consecutive_scan_errors'],1)
            r.close()

    def test_corrupt_pool_log_does_not_advance_cursor(self):
        with TemporaryDirectory() as d:
            c,a=self.fixture(d)
            creation=self.creation();bad=self.event('v3_swap');bad['data']='0x123'
            class Fake:
                def rpc(self,method,params):
                    if method=='eth_getLogs':
                        return ([creation] if UNI_FACTORY in params[0]['address'] else [bad]),200
                    if method=='eth_getBlockByNumber':
                        return {'number':hex(101),'timestamp':hex(int(time.time())-4),'hash':self_outer.w(555)},200
                    raise AssertionError(method)
            self_outer=self
            scan_ranges(c,Fake(),a,105)
            self.assertEqual(campaign(c)['next_block'],101)
            self.assertEqual(report(c)['issues']['scan_error_DataError'],1)
            c.close()


class V03Tests(unittest.TestCase):
    def setUp(self):
        self.old=Tests()

    def test_anchor_classification_predeclared(self):
        target='0x'+'c'*40
        self.assertEqual(classify_pair(USDC,target),('direct_usdc',USDC,target))
        self.assertEqual(classify_pair(target,WETH),('direct_weth',WETH,target))
        self.assertEqual(classify_pair(USDC,WETH),('both_anchors',None,None))
        self.assertEqual(classify_pair('0x'+'c'*40,'0x'+'d'*40),('non_anchor',None,None))

    def test_v3_quoter_selector_abi_static_tuple(self):
        to,payload,label=quote_calldata('uniswap_v3','0x'+'b'*40,USDC,WETH,
                                        10**7,jdump({'fee':500}))
        self.assertEqual(to,QUOTER_V2)
        self.assertEqual(payload[:10],selector('quoteExactInputSingle((address,address,uint256,uint24,uint160))'))
        self.assertEqual(len(payload),10+5*64)
        self.assertEqual(int(payload[10+3*64:10+4*64],16),500)
        self.assertIn('quoter',label)

    def test_aero_calldata_and_selector(self):
        pool='0x'+'a'*40
        to,payload,name=quote_calldata('aerodrome_classic',pool,USDC,WETH,1000,'{}')
        self.assertEqual((to,name),(pool,'aerodrome_pool_getAmountOut'))
        self.assertEqual(payload[:10],selector('getAmountOut(uint256,address)'))
        self.assertEqual(len(payload),10+128)

    def test_quoter_output_validated(self):
        v=parse_v3_quote('0x'+''.join(abi_word(x) for x in (1000,2**96,2,170000)))
        self.assertEqual(v['amount_out'],1000)
        self.assertEqual(v['ticks'],2)
        self.assertEqual(parse_aero_quote('0x'+abi_word(123))['amount_out'],123)
        with self.assertRaises(DataError):parse_v3_quote('0x'+''.join(abi_word(x) for x in (0,2**96,0,1)))
        with self.assertRaises(DataError):parse_aero_quote('0x')

    def test_strict_decimals_and_size(self):
        self.assertEqual(raw_for_size(USDC,'10',6),10_000_000)
        self.assertEqual(raw_for_size(WETH,'0.005',18),5*10**15)
        with self.assertRaises(DataError):raw_for_size(USDC,'0.005',1)
        with self.assertRaises(DataError):raw_for_size(WETH,'1',40)

    def test_v3_spot_price_formula_and_inverse(self):
        self.assertEqual(Decimal(price_token1_per_token0(2**96,18,6)),Decimal(10)**12)
        self.assertEqual(Decimal(price_token1_per_token0(2**96,6,18)),Decimal(10)**-12)
        self.assertEqual(Decimal(price_token1_per_token0(2**95,18,18)),Decimal('0.25'))
        with self.assertRaises(DataError):price_token1_per_token0(0,18,6)

    def test_new_pool_anchor_cohort_and_schedule_after_observed_swap(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            p=decode_creation(self.old.creation());p['token0']=USDC;p['token1']='0x'+'c'*40
            p['block_time_ms']=now_ms()-20_000;p['observed_ms']=now_ms()-5_000
            e=decode_event(self.old.event('v3_swap'),'uniswap_v3',p['pool']);e['block_time_ms']=p['block_time_ms'];e['observed_ms']=p['observed_ms']+500
            persist_range(c,[p],[e],101,101,now_ms(),a)
            initialize_quote_schedule(c)
            initialize_quote_schedule(c)
            row=c.execute('SELECT * FROM pool_cohorts').fetchone()
            self.assertEqual(row['cohort'],'direct_usdc')
            self.assertEqual(row['first_swap_seen_ms'],e['observed_ms'])
            self.assertEqual(c.execute('SELECT COUNT(*) FROM quote_schedule').fetchone()[0],10)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM quote_schedule WHERE status="waiting_buy"').fetchone()[0],6)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM token_metadata').fetchone()[0],2)
            self.assertIsNone(c.execute('SELECT MIN(response_received_ms) FROM quote_schedule').fetchone()[0])
            c.close()

    def test_non_anchor_never_enters_quote_denominator(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            p=decode_creation(self.old.creation());p['block_time_ms']=now_ms()-1000
            ev=decode_event(self.old.event('v3_swap'),'uniswap_v3',p['pool']);ev['block_time_ms']=p['block_time_ms']
            persist_range(c,[p],[ev],101,101,now_ms(),a)
            initialize_quote_schedule(c)
            rr=quote_report(c)
            self.assertEqual(rr['direct_anchor_eligible'],0)
            self.assertEqual(rr['cohort_counts']['non_anchor'],1)
            self.assertEqual(rr['reasons']['unsupported_route_v03a'],1)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM quote_schedule').fetchone()[0],0)
            c.close()

    def test_buy_sell_prospective_abi_mock_and_no_pnl(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d,eth_call_per_minute=6,max_quote_calls_per_cycle=2)
            now=now_ms();p=decode_creation(self.old.creation());p['token0']=USDC;p['token1']='0x'+'c'*40
            p['block_time_ms']=now-45_000;p['observed_ms']=now-25_000
            ev=decode_event(self.old.event('v3_swap'),'uniswap_v3',p['pool']);ev['block_time_ms']=p['block_time_ms'];ev['observed_ms']=now-23_000
            persist_range(c,[p],[ev],101,101,now,a)
            initialize_quote_schedule(c)
            class Mock:
                def rpc(self,method,params):
                    if method=='eth_call':
                        obj=params[0];dest=obj['to']
                        if obj['data']==selector('decimals()'):
                            return ('0x'+abi_word(6 if dest==USDC else 18)),200
                        if dest==QUOTER_V2:
                            return '0x'+abi_word(10**18)+abi_word(2**96)+abi_word(0)+abi_word(100000),200
                        if obj['data']==selector('slot0()'):
                            return '0x'+''.join(abi_word(x) for x in (2**96,0,0,0,0,0,1)),200
                        if obj['data']==selector('liquidity()'):
                            return '0x'+abi_word(999),200
                    if method=='eth_getBlockByNumber':
                        return {'number':params[0],'timestamp':hex(int(time.time())-15),
                                'hash':self_outer.old.w(555)},200
                    raise AssertionError(method)
            self_outer=self
            m=Mock()
            # Two metadata calls followed by two due +15 second BUY sizes.
            self.assertEqual(process_quote_jobs(c,m,a,110),2)
            self.assertEqual(process_quote_jobs(c,m,a,110),2)
            out=quote_report(c)
            self.assertEqual(out['success_by_side']['buy'],2)
            self.assertEqual(out['buy_120s_covered_pools'],1)
            self.assertEqual(out['job_status']['pending'],8)
            sells=list(c.execute('SELECT * FROM quote_schedule WHERE side="sell"'))
            self.assertEqual(len(sells),6)
            self.assertTrue(all(z['buy_job_id'] for z in sells))
            self.assertFalse(out['quotes_suspended'])
            self.assertEqual(out['eth_call_requests'],4)
            self.assertNotIn('expectancy',out)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM quotes WHERE side="sell"').fetchone()[0],0)
            c.close()

    def test_eth_call_kill_switch_leaves_scan_independent(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d,eth_call_per_minute=30)
            class Nope:
                def rpc(self,method,params):raise TransportError('http_429',429)
            for i in range(5):
                with self.assertRaises(TransportError):
                    ethcall(c,Nope(),USDC,selector('decimals()'),101,'unit',a)
            self.assertTrue(quote_suspended(c))
            self.assertEqual(campaign(c)['phase'],'running')
            self.assertEqual(quote_report(c)['eth_call_errors'],5)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM issues WHERE category="quote_kill_switch"').fetchone()[0],1)
            c.close()

    def test_censored_tasks_do_not_manufacture_zero_quote(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            p=decode_creation(self.old.creation());p['token0']=WETH;p['token1']='0x'+'d'*40;p['block_time_ms']=now_ms()-1000
            ev=decode_event(self.old.event('v3_swap'),'uniswap_v3',p['pool']);ev['block_time_ms']=p['block_time_ms']
            persist_range(c,[p],[ev],101,101,now_ms(),a)
            initialize_quote_schedule(c)
            finalize_quote_jobs(c,'campaign_end')
            out=quote_report(c)
            self.assertEqual(out['job_status']['censored'],10)
            self.assertEqual(out['success_by_side'],{})
            self.assertEqual(out['direct_anchor_eligible'],1)
            self.assertTrue(out['all_missing_are_censored'])
            c.close()

    def test_read_only_method_whitelist_blocks_sending(self):
        transport=Transport(DEFAULT_RPC,5)
        for method in ('eth_sendRawTransaction','eth_sendTransaction','personal_sign','eth_sign'):
            with self.assertRaises(DataError):transport.rpc(method,[])

    def test_quote_parser_rejects_malformed_and_extreme_values(self):
        with self.assertRaises(DataError):abi_output('0x'*33,1)
        with self.assertRaises(DataError):parse_v3_quote('0x'+''.join(abi_word(x) for x in (1,2**160,0,0)))
        with self.assertRaises(DataError):abi_data('getAmountOut(uint256,address)',-1,USDC)

    def test_contract_smoke_full_mock_has_no_transacting_calls(self):
        with TemporaryDirectory() as d:
            a=self.old.args(db=str(Path(d)/'contract_smoke.db'),eth_call_per_minute=6)
            pool='0x'+'a'*40
            requests=[]
            class Fake:
                def rpc(self,method,params):
                    requests.append(method)
                    if method=='eth_chainId':return hex(CHAIN),200
                    if method=='eth_blockNumber':return hex(111),200
                    if method=='eth_getCode':return '0x6001600055',200
                    if method!='eth_call':raise AssertionError(method)
                    obj=params[0]
                    if obj['to']==UNI_FACTORY:return '0x'+abi_word(int(pool,16)),200
                    if obj['data']==selector('decimals()'):
                        return '0x'+abi_word(18 if obj['to']==WETH else 6),200
                    if obj['data']==selector('slot0()'):
                        return '0x'+''.join(abi_word(x) for x in (2**96,0,0,0,0,0,1)),200
                    if obj['data']==selector('liquidity()'):return '0x'+abi_word(10000),200
                    if obj['to']==QUOTER_V2:
                        return '0x'+''.join(abi_word(x) for x in (12345,2**96,1,100)),200
                    raise AssertionError(obj)
            original=globals()['Transport']
            try:
                globals()['Transport']=lambda *x:Fake()
                self.assertEqual(contract_smoke(a),0)
            finally:globals()['Transport']=original
            self.assertNotIn('eth_sendRawTransaction',requests)
            self.assertEqual(requests.count('eth_call'),6)
            db=connect(Path(a.db),True)
            self.assertEqual(campaign(db)['phase'],'complete')
            self.assertEqual(quote_report(db)['eth_call_requests'],6)
            db.close()

    def test_pool_state_read_separate_from_executable_quote(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d,eth_call_per_minute=6,max_quote_calls_per_cycle=2)
            now=now_ms();p=decode_creation(self.old.creation());p['token0']=USDC;p['token1']='0x'+'c'*40
            p['block_time_ms']=now-45_000;p['observed_ms']=now-25_000
            ev=decode_event(self.old.event('v3_swap'),'uniswap_v3',p['pool'])
            ev['block_time_ms']=p['block_time_ms'];ev['observed_ms']=now-23_000
            persist_range(c,[p],[ev],101,101,now,a);initialize_quote_schedule(c)
            class Fake:
                def rpc(self,method,params):
                    if method=='eth_getBlockByNumber':
                        return {'number':params[0],'timestamp':hex(int(time.time())-12),
                                'hash':self_outer.old.w(555)},200
                    obj=params[0];selector_in=obj['data']
                    if selector_in==selector('decimals()'):
                        return '0x'+abi_word(6 if obj['to']==USDC else 18),200
                    if obj['to']==QUOTER_V2:
                        return '0x'+''.join(abi_word(x) for x in (10**17,2**96,0,100000)),200
                    if selector_in==selector('slot0()'):
                        return '0x'+''.join(abi_word(x) for x in (2**96,0,0,0,0,0,1)),200
                    if selector_in==selector('liquidity()'):
                        return '0x'+abi_word(999),200
                    raise AssertionError(params)
            self_outer=self
            for _ in range(3):process_quote_jobs(c,Fake(),a,110)
            st=c.execute('SELECT * FROM pool_state').fetchone()
            self.assertEqual(st['status'],'complete')
            self.assertEqual(st['active_liquidity_raw'],'999')
            self.assertEqual(Decimal(st['spot_token1_per_token0']),Decimal('1e-12'))
            self.assertEqual(c.execute('SELECT COUNT(*) FROM quotes').fetchone()[0],2)
            c.close()

    def test_persisted_exact_amount_raw_never_float(self):
        raw=raw_for_size(USDC,'50',6)
        self.assertEqual(str(raw),'50000000')
        self.assertIsInstance(raw,int)


class V03a1Tests(unittest.TestCase):
    def setUp(self):
        self.old = Tests()

    def test_version_schema_and_unchanged_quote_plan(self):
        self.assertEqual((VERSION, SCHEMA), ('0.3a.1',4))
        self.assertEqual(BUY_DELAYS,(15,60))
        self.assertEqual(SELL_HORIZONS,(5,15,30))
        self.assertEqual(SIZES['weth'],('0.005','0.02'))
        a=parse_args(['--db','some_new.sqlite3','--drain-minutes','90'])
        validate(a)
        self.assertEqual(a.eth_call_per_minute,6)
        self.assertEqual(a.max_quote_lateness_seconds,120)
        self.assertEqual(a.watch_minutes,30)
        self.assertEqual(a.confirmations,3)
        self.assertEqual(a.drain_minutes,90)

    def test_rpc_diagnostic_never_persists_freeform_secrets(self):
        api_secret='MY_SUPER_SECRET_EXAMPLE_123456789'
        data='0xdeadbeef'+'a'*64
        x=safe_rpc_diagnostic({'code':3,'message':'execution reverted https://sensitive.provider.net/'+api_secret,
                               'data':{'originalError':{'data':data}},'other':api_secret})
        dump=jdump(x)
        self.assertEqual(x['rpc_code'],3)
        self.assertEqual(x['selector'],'0xdeadbeef')
        self.assertEqual(x['message_class'],'execution_reverted')
        self.assertNotIn(api_secret,dump)
        self.assertNotIn('https',dump)
        self.assertNotIn(data,dump)
        self.assertEqual(len(x['data_sha256']),64)

    def test_known_revert_reason_decoded_allowlist_only(self):
        def make_error_string(reason):
            raw=reason.encode()
            return '0x08c379a0'+abi_word(32)+abi_word(len(raw))+raw.hex().ljust(64*((len(raw)+31)//32),'0')
        x=safe_rpc_diagnostic({'code':3,'message':'execution reverted',
                               'data':make_error_string('STF')})
        self.assertEqual(x['allowlisted_reason'],'STF')
        x2=safe_rpc_diagnostic({'code':3,'message':'execution reverted',
                                'data':make_error_string('PRIVATE_EXAMPLE_KEY_VALUE')})
        self.assertNotIn('allowlisted_reason',x2)
        self.assertNotIn('PRIVATE_EXAMPLE_KEY_VALUE',jdump(x2))
        self.assertEqual(x2['selector'],'0x08c379a0')

    def test_panic_and_malformed_diagnostic(self):
        x=safe_rpc_diagnostic({'code':3,'message':'out of gas',
                               'data':'0x4e487b71'+abi_word(17)})
        self.assertEqual((x['message_class'],x['panic_code']),('out_of_gas','0x11'))
        x=safe_rpc_diagnostic({'code':'http://secret','message':'Bearer token',
                               'data':{'nested':'http://secret'}})
        self.assertNotIn('rpc_code',x)
        self.assertNotIn('Bearer',jdump(x))
        self.assertNotIn('secret',jdump(x))

    def test_transport_rpc_preserves_safe_diagnostic(self):
        t=Transport(DEFAULT_RPC,5)
        t.request=lambda url,obj=None,gecko=False: ({'jsonrpc':'2.0','id':1,
            'error':{'code':3,'message':'execution reverted: STF','data':'0xdeadbeef'}},200)
        with self.assertRaises(TransportError) as raised:
            t.rpc('eth_call',[{'to':WETH,'data':'0x313ce567'},'latest'])
        self.assertEqual(raised.exception.diagnostic['rpc_code'],3)
        self.assertEqual(raised.exception.diagnostic['allowlisted_reason'],'STF')
        self.assertEqual(raised.exception.status,200)

    def test_failure_request_persisted_and_linked_to_job(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d,eth_call_per_minute=30,max_quote_calls_per_cycle=6)
            now=now_ms()
            p=decode_creation(self.old.creation())
            p['token0']=WETH;p['token1']='0x'+'c'*40
            p['block_time_ms']=now-140_000;p['observed_ms']=now-130_000
            ev=decode_event(self.old.event('v3_swap'),'uniswap_v3',p['pool'])
            ev['block_time_ms']=p['block_time_ms']
            ev['observed_ms']=now-120_000
            persist_range(c,[p],[ev],101,101,now,a)
            initialize_quote_schedule(c)
            class Fake:
                def rpc(self,method,params):
                    if method=='eth_getBlockByNumber':
                        return {'number':params[0],'timestamp':hex(int(time.time())-10),
                                'hash':self_outer.old.w(555)},200
                    obj=params[0]
                    if obj['data']==selector('decimals()'):
                        return ('0x'+abi_word(18)),200
                    if obj['to']==QUOTER_V2:
                        amount=int(obj['data'][10+2*64:10+3*64],16)
                        if amount in (5*10**15,2*10**16):
                            raise TransportError('rpc_error_3',200,diagnostic=safe_rpc_diagnostic(
                                {'code':3,'message':'execution reverted: STF',
                                 'data':'0xdeadbeef'+abi_word(123)}))
                        return '0x'+''.join(abi_word(x) for x in (10**16,2**96,0,10000)),200
                    raise AssertionError(obj)
            self_outer=self
            for _ in range(3):
                process_quote_jobs(c,Fake(),a,110)
            failed=list(c.execute('''SELECT s.request_id,r.success,r.method,r.error_diag_json
                    FROM quote_schedule s JOIN requests r ON r.id=s.request_id
                    WHERE s.status="failed" AND s.side="buy"'''))
            self.assertEqual(len(failed),4) # two sizes x two delays
            self.assertTrue(all(r['request_id'] is not None for r in failed))
            self.assertTrue(all(r['success']==0 and r['method']=='eth_call' for r in failed))
            self.assertTrue(all(json.loads(r['error_diag_json'])['rpc_code']==3 for r in failed))
            audit=audit_quotes(c,p['pool'])
            self.assertEqual(audit['problem_jobs_count'],10)  # 4 BUY failures plus 6 dependent SELL censors
            self.assertTrue(all(j['linked_rpc_request']['safe_diagnostic']['allowlisted_reason']=='STF'
                                for j in audit['pools'][0]['problem_jobs'] if j['status']=='failed'))
            c.close()

    def test_successful_ethcall_has_request_link(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            class Fake:
                def rpc(self,m,params):
                    return '0x'+abi_word(18),200
            result,when,reqid=ethcall(c,Fake(),WETH,selector('decimals()'),100,'test',a)
            self.assertEqual(result,'0x'+abi_word(18))
            self.assertEqual(c.execute('SELECT success FROM requests WHERE id=?',(reqid,)).fetchone()[0],1)
            c.close()

    def test_legacy_03a_audit_without_mutating_database(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            pool='0x'+'a'*40
            # Simulate a legacy schema identifier using a compatible in-memory DB;
            # the audit only SELECTs columns available on real schema-3 databases.
            with c:
                c.execute('UPDATE campaign SET version="0.3a",schema_v=3')
                c.execute('''INSERT INTO pools(pool,dex,factory,token0,token1,meta_json,block_number,
                    block_hash,block_time_ms,creation_tx,creation_index,first_seen_ms,initial_state,current_state)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (pool,'uniswap_v3',UNI_FACTORY,WETH,'0x'+'c'*40,'{}',101,self.old.w(555),now_ms()-10000,self.old.w(2),0,now_ms()-9000,'tracked','expired'))
                c.execute('''INSERT INTO quote_schedule(pool,side,size_key,buy_delay_s,horizon_min,due_ms,created_ms,status,reason,state_block,request_started_ms)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                            (pool,'buy','0.005',60,0,now_ms()-5000,now_ms()-10000,'failed','rpc_error_3',102,now_ms()-4000))
                c.execute('''INSERT INTO events(pool,event_kind,block_number,block_hash,block_time_ms,tx_hash,log_index,observed_ms,raw_json,within_watch)
                            VALUES(?,?,?,?,?,?,?,?,?,?)''',
                            (pool,'v3_burn',102,self.old.w(555),now_ms()-7000,self.old.w(3),2,now_ms()-6000,'{}',1))
            c.close()
            db=Path(a.db)
            before=(db.stat().st_size,db.stat().st_mtime_ns)
            ro=connect(db,True)
            x=audit_quotes(ro)
            self.assertEqual(x['source_version'],'0.3a')
            self.assertEqual(x['problem_jobs_count'],1)
            j=x['pools'][0]['problem_jobs'][0]
            self.assertIsNone(j['linked_rpc_request'])
            self.assertEqual(j['mint_burn_swap_context']['burn']['state_block_at_or_before'],1)
            self.assertEqual(j['mint_burn_swap_context']['burn']['observed_before_request'],1)
            ro.close()
            self.assertEqual(before,(db.stat().st_size,db.stat().st_mtime_ns))

    def test_censored_job_has_no_fabricated_request(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            p=decode_creation(self.old.creation())
            p['block_time_ms']=now_ms()-10_000
            persist_range(c,[p],[],101,101,now_ms(),a)
            with c:
                c.execute('''INSERT INTO quote_schedule(pool,side,size_key,buy_delay_s,horizon_min,
                    due_ms,created_ms,status,reason) VALUES(?,?,?,?,?,?,?,?,?)''',
                    (p['pool'],'sell','0.005',15,30,now_ms()+10000,now_ms(),'censored','campaign_end'))
            x=audit_quotes(c,p['pool'])
            j=x['pools'][0]['problem_jobs'][0]
            self.assertEqual(j['status'],'censored')
            self.assertIsNone(j['linked_rpc_request'])
            self.assertIsNone(j['mint_burn_swap_context']['burn']['state_block_at_or_before'])
            c.close()

    def test_readonly_audit_cli_does_not_touch_original(self):
        with TemporaryDirectory() as d:
            c,a=self.old.fixture(d)
            with c:
                c.execute('UPDATE campaign SET phase="complete",completed_ms=?',(now_ms(),))
            c.close()
            before=Path(a.db).stat().st_mtime_ns
            self.assertEqual(main(['--audit','--db',a.db]),0)
            self.assertEqual(before,Path(a.db).stat().st_mtime_ns)
            self.assertEqual(main(['--report','--db',a.db]),0)



def parse_args(argv=None):
    p=argparse.ArgumentParser(description='Base executability quotes and diagnostic feasibility V0.3a.1 (READ ONLY)')
    p.add_argument('--db',default='data/dex_early_executability_v03a1.sqlite3')
    p.add_argument('--rpc-url',default=os.getenv('BASE_RPC_URL',DEFAULT_RPC),help='HTTPS Base RPC; prefer BASE_RPC_URL env for any private provider keys')
    p.add_argument('--duration-hours',type=float,default=6)
    p.add_argument('--poll-seconds',type=float,default=10)
    p.add_argument('--confirmations',type=int,default=3)
    p.add_argument('--max-blocks-per-range',type=int,default=5)
    p.add_argument('--max-ranges-per-cycle',type=int,default=3)
    p.add_argument('--max-active-pools',type=int,default=16)
    p.add_argument('--max-pool-addresses-per-query',type=int,default=24)
    p.add_argument('--watch-minutes',type=int,default=30)
    p.add_argument('--drain-minutes',type=int,default=40)
    p.add_argument('--dex-requests-per-minute',type=int,default=12)
    p.add_argument('--gecko-requests-per-minute',type=int,default=2)
    p.add_argument('--dex-spacing-seconds',type=int,default=60)
    p.add_argument('--gecko-spacing-seconds',type=int,default=120)
    p.add_argument('--max-provider-calls-per-cycle',type=int,default=3)
    p.add_argument('--http-timeout',type=float,default=10)
    p.add_argument('--max-consecutive-scan-errors',type=int,default=5,
                   help='Mark campaign incomplete and stop after repeated scan errors; never silently skip blocks')
    p.add_argument('--no-providers',action='store_true',help='Measure on-chain events only')
    p.add_argument('--no-quotes',action='store_true',help='Skip metadata and eth_call quote feasibility')
    p.add_argument('--eth-call-per-minute',type=int,default=6,help='Global eth_call budget, incl. decimals')
    p.add_argument('--max-quote-calls-per-cycle',type=int,default=2)
    p.add_argument('--max-quote-lateness-seconds',type=int,default=120)
    p.add_argument('--contract-smoke',action='store_true',help='Isolated live known-pool QuoterV2 smoke; NEW DB')
    p.add_argument('--resume',action='store_true')
    p.add_argument('--once',action='store_true')
    p.add_argument('--report',action='store_true',help='READ ONLY local SQLite report, no RPC')
    p.add_argument('--audit',action='store_true',help='READ ONLY local problematic quote/event chronology; accepts legacy V0.3a')
    p.add_argument('--audit-pool',help='Optional pool address to filter --audit; no RPC')
    p.add_argument('--self-test',action='store_true')
    return p.parse_args(argv)

def validate(a):
    check=[('duration_hours',0<a.duration_hours<=168),
       ('poll_seconds',2<=a.poll_seconds<=300),('confirmations',1<=a.confirmations<=100),
       ('max_blocks_per_range',1<=a.max_blocks_per_range<=10),
       ('max_ranges_per_cycle',1<=a.max_ranges_per_cycle<=20),
       ('max_active_pools',1<=a.max_active_pools<=100),
       ('max_pool_addresses_per_query',a.max_active_pools<=a.max_pool_addresses_per_query<=100),
       ('watch_minutes',1<=a.watch_minutes<=120),
       ('drain_minutes',a.watch_minutes+5<=a.drain_minutes<=240),
       ('dex_requests_per_minute',0<=a.dex_requests_per_minute<=60),
       ('gecko_requests_per_minute',0<=a.gecko_requests_per_minute<=10),
       ('dex_spacing_seconds',10<=a.dex_spacing_seconds<=3600),
       ('gecko_spacing_seconds',10<=a.gecko_spacing_seconds<=3600),
       ('max_provider_calls_per_cycle',1<=a.max_provider_calls_per_cycle<=20),
       ('http_timeout',2<=a.http_timeout<=60),
       ('max_consecutive_scan_errors',1<=a.max_consecutive_scan_errors<=100),
       ('eth_call_per_minute',0<=a.eth_call_per_minute<=30),
       ('max_quote_calls_per_cycle',1<=a.max_quote_calls_per_cycle<=6),
       ('max_quote_lateness_seconds',15<=a.max_quote_lateness_seconds<=600)]
    for name,ok in check:
        if not ok:raise DataError('Invalid parameter: '+name)

def main(argv=None):
    a=parse_args(argv)
    if a.self_test:
        suite=unittest.TestSuite((
            unittest.defaultTestLoader.loadTestsFromTestCase(Tests),
            unittest.defaultTestLoader.loadTestsFromTestCase(V03Tests),
            unittest.defaultTestLoader.loadTestsFromTestCase(V03a1Tests)))
        res=unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if res.wasSuccessful() else 1
    if a.audit and a.report:
        raise DataError('Choose only one of --report and --audit')
    if a.audit_pool and not a.audit:
        raise DataError('--audit-pool requires --audit')
    if a.audit:
        c=connect(Path(a.db),True)
        try:print(json.dumps(audit_quotes(c,a.audit_pool),indent=2))
        finally:c.close()
        return 0
    if a.report:
        c=connect(Path(a.db),True)
        try:print(json.dumps(report(c),indent=2))
        finally:c.close()
        return 0
    validate(a)
    if a.contract_smoke:
        if a.resume or a.once:raise DataError('Contract smoke cannot combine --resume or --once')
        return contract_smoke(a)
    return run(a)

if __name__=='__main__':
    try:sys.exit(main())
    except (DataError,TransportError,sqlite3.Error,OSError,ValueError) as e:
        # Avoid printing any RPC URL; it may embed an API key.
        print('ERROR '+type(e).__name__+(': '+str(e) if isinstance(e,DataError) and 'URL' not in str(e) else ''),file=sys.stderr)
        sys.exit(2)
