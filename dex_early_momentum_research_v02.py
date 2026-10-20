#!/usr/bin/env python3
"""DEX Early Momentum Research V0.2: Base on-chain event observability.

Standard library only. READ ONLY on Base, no wallet, private key, signature,
eth_send*, transaction, trade, price prediction, or execution PnL.

Scopes two factory contracts: Uniswap V3 and Aerodrome classic. Discovers
prospective pool creation, watches raw Mint/Swap plus removal/initialization
or reserve-sync events, and optionally polls DexScreener and GeckoTerminal.
Requires a Base HTTPS RPC that permits eth_getLogs on small block ranges.
"""
from __future__ import annotations

import argparse
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

VERSION = "0.2"
CHAIN = 8453
SCHEMA = 1
UNI_FACTORY = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
AERO_FACTORY = "0x420dd381b31aef6683db6b902084cb0ffece40da"
DEFAULT_RPC = "https://base-mainnet.g.alchemy.com/public"
DEX_URL = "https://api.dexscreener.com/latest/dex/pairs/base/"
GECKO_URL = "https://api.geckoterminal.com/api/v2/networks/base/pools/"
HEADERS = {"User-Agent": "DEX-Early-Onchain/0.2 research read-only", "Accept": "application/json"}
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
    def __init__(self,kind,status=None):super().__init__(kind);self.kind=kind;self.status=status

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
        if method not in ('eth_chainId','eth_blockNumber','eth_getLogs','eth_getBlockByNumber'):
            raise DataError('Method not on READ ONLY whitelist')
        self.seq+=1
        data,status=self.request(self.url,{'jsonrpc':'2.0','method':method,'params':params,'id':self.seq})
        if not isinstance(data,dict) or data.get('id')!=self.seq or data.get('jsonrpc')!='2.0':raise TransportError('invalid_envelope',status)
        if data.get('error'):
            code=data['error'].get('code') if isinstance(data['error'],dict) else None
            raise TransportError('rpc_error_'+str(code),status)
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
 error_kind TEXT,result_n INTEGER);
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
CREATE INDEX pool_state ON pools(current_state,first_seen_ms);
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
"""

def settings(a:argparse.Namespace)->dict:
    # Endpoints/API secrets omitted from DB configuration and logs.
    return {k:getattr(a,k) for k in ('duration_hours','poll_seconds','confirmations',
        'max_blocks_per_range','max_ranges_per_cycle','max_active_pools','watch_minutes',
        'drain_minutes','max_pool_addresses_per_query','dex_requests_per_minute',
        'gecko_requests_per_minute','dex_spacing_seconds','gecko_spacing_seconds',
        'max_provider_calls_per_cycle','http_timeout','no_providers','max_consecutive_scan_errors')}

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

def call(c,transport:Transport,source:str,method:str,params=None,pool=None):
    began=now_ms();data=None;http=None;err=None;success=False
    try:
        if source=='rpc':data,http=transport.rpc(method,params or [])
        elif source=='dex':data,http=transport.request(DEX_URL+pool)
        elif source=='gecko':data,http=transport.request(GECKO_URL+pool,gecko=True)
        else:raise DataError('Invalid request source')
        success=True
    except TransportError as e:err=e.kind;http=e.status
    except (OSError,DataError) as e:err=type(e).__name__
    ended=now_ms()
    cur=c.execute('INSERT INTO requests(source,method,pool,started_ms,ended_ms,success,http_status,error_kind,result_n) VALUES (?,?,?,?,?,?,?,?,?)',
       (source,method,pool,began,ended,int(success),http,err,len(data) if isinstance(data,list) else None))
    c.commit()
    if not success:raise TransportError(err or 'unknown',http)
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
          'Paired on-chain/provider lead compares timestamps of two different sampled observations, NOT executable prices']}
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
    else:
        if path.exists():raise DataError('A new run must use a new DB filename')
        c=None
    try:
        ident,_=transport.rpc('eth_chainId',[])
        if int(ident,16)!=CHAIN:raise DataError('Not Base mainnet (chainId 8453)')
        if c is None:
            h,_=transport.rpc('eth_blockNumber',[])
            c=create_db(path,a,int(h,16),now_ms())
        print(f'DEX EARLY MOMENTUM RESEARCH V{VERSION} READ ONLY | chain=8453 | DB={a.db} | start_block={campaign(c)["start_block"]}',flush=True)
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
                print(f'PILOT HALTED: {consecutive_scan_errors} consecutive scan failures; cursor not advanced',flush=True)
                break
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
            print(f'loop={loops} phase={campaign(c)["phase"]} next_block={campaign(c)["next_block"]} '
                  f'new_tracked={admitted} excluded={excluded} new_pool_events={evcount} provider_calls={providers}',flush=True)
            if a.once or campaign(c)['phase'] in ('complete','incomplete_backlog'):break
            time.sleep(a.poll_seconds)
        print('Final/current report:',jdump(report(c)),flush=True)
        return 2 if campaign(c)['phase']=='incomplete_backlog' else 0
    finally:
        if c is not None:c.close()

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


def parse_args(argv=None):
    p=argparse.ArgumentParser(description='Base pool discovery + Mint/Swap event research (READ ONLY)')
    p.add_argument('--db',default='data/dex_early_momentum_research_v02.sqlite3')
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
    p.add_argument('--resume',action='store_true')
    p.add_argument('--once',action='store_true')
    p.add_argument('--report',action='store_true',help='READ ONLY local SQLite report, no RPC')
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
       ('max_consecutive_scan_errors',1<=a.max_consecutive_scan_errors<=100)]
    for name,ok in check:
        if not ok:raise DataError('Invalid parameter: '+name)

def main(argv=None):
    a=parse_args(argv)
    if a.self_test:
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        res=unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if res.wasSuccessful() else 1
    if a.report:
        c=connect(Path(a.db),True)
        try:print(json.dumps(report(c),indent=2))
        finally:c.close()
        return 0
    validate(a)
    return run(a)

if __name__=='__main__':
    try:sys.exit(main())
    except (DataError,TransportError,sqlite3.Error,OSError,ValueError) as e:
        # Avoid printing any RPC URL; it may embed an API key.
        print('ERROR '+type(e).__name__+(': '+str(e) if isinstance(e,DataError) and 'URL' not in str(e) else ''),file=sys.stderr)
        sys.exit(2)
