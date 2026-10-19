#!/usr/bin/env python3
"""DEX Early Discovery Feasibility V0.1 — Base READ ONLY prospectively.

Standard library only. No wallet, keys, signing, eth_sendRawTransaction or trades.
Tracks Uniswap V3 and Aerodrome classic PoolCreated events, then checks whether
selected new pools appear on DexScreener and GeckoTerminal. This is an API/RPC
feasibility experiment, NOT a momentum signal generator or trading backtest.
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
import urllib.error
import urllib.parse
import urllib.request
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

VERSION = "0.1"
SCHEMA = 1
CHAIN_ID = 8453
UNISWAP_V3_FACTORY = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
AERODROME_CLASSIC_FACTORY = "0x420dd381b31aef6683db6b902084cb0ffece40da"
RPC_PUBLIC = "https://mainnet.base.org"
DEX_URL = "https://api.dexscreener.com/latest/dex/pairs/base/"
GECKO_URL = "https://api.geckoterminal.com/api/v2/networks/base/pools/"
HEADERS = {"User-Agent": "DEX-Early-Feasibility/0.1 (research-read-only)",
           "Accept": "application/json"}
GECKO_HEADERS = {**HEADERS, "Accept": "application/json;version=20230203"}

# Minimal independent Keccak-256 (Ethereum, NOT NIST SHA3-256).
_ROUNDS = [0x0000000000000001, 0x0000000000008082, 0x800000000000808a,
           0x8000000080008000, 0x000000000000808b, 0x0000000080000001,
           0x8000000080008081, 0x8000000000008009, 0x000000000000008a,
           0x0000000000000088, 0x0000000080008009, 0x000000008000000a,
           0x000000008000808b, 0x800000000000008b, 0x8000000000008089,
           0x8000000000008003, 0x8000000000008002, 0x8000000000000080,
           0x000000000000800a, 0x800000008000000a, 0x8000000080008081,
           0x8000000000008080, 0x0000000080000001, 0x8000000080008008]
_ROT = [0, 1, 62, 28, 27, 36, 44, 6, 55, 20, 3, 10, 43, 25, 39,
        41, 45, 15, 21, 8, 18, 2, 61, 56, 14]
_MASK = (1 << 64) - 1


def keccak256(raw: bytes) -> bytes:
    def rol(x: int, n: int) -> int:
        return ((x << n) | (x >> (64 - n))) & _MASK if n else x

    def permute(a: list[int]) -> None:
        for rc in _ROUNDS:
            c = [a[x] ^ a[x+5] ^ a[x+10] ^ a[x+15] ^ a[x+20]
                 for x in range(5)]
            d = [c[(x-1) % 5] ^ rol(c[(x+1) % 5], 1) for x in range(5)]
            for y in range(5):
                for x in range(5):
                    a[x+5*y] ^= d[x]
            b = [0] * 25
            for y in range(5):
                for x in range(5):
                    b[y + 5 * ((2*x + 3*y) % 5)] = rol(a[x+5*y], _ROT[x+5*y])
            for y in range(5):
                for x in range(5):
                    a[x+5*y] = b[x+5*y] ^ ((~b[(x+1) % 5 + 5*y]) & b[(x+2) % 5 + 5*y])
            a[0] ^= rc

    rate = 136
    buf = bytearray(raw)
    buf.append(0x01)
    while len(buf) % rate != rate-1:
        buf.append(0)
    buf.append(0x80)
    state = [0]*25
    for off in range(0, len(buf), rate):
        for i in range(rate//8):
            state[i] ^= int.from_bytes(buf[off+8*i:off+8*i+8], 'little')
        permute(state)
    return b''.join(x.to_bytes(8, 'little') for x in state)[:32]


UNI_TOPIC = '0x' + keccak256(b'PoolCreated(address,address,uint24,int24,address)').hex()
AERO_TOPIC = '0x' + keccak256(b'PoolCreated(address,address,bool,address,uint256)').hex()
assert UNI_TOPIC == '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118', 'Keccak implementation error'


class DataError(RuntimeError):
    pass


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def compact_json(x: Any) -> str:
    return json.dumps(x, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def sha256(x: Any) -> str:
    return hashlib.sha256(compact_json(x).encode()).hexdigest()


def addr(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}', value):
        raise DataError('Invalid Ethereum address')
    return value.lower()


def word(data: str, index: int) -> int:
    if not isinstance(data, str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{64})*', data):
        raise DataError('Malformed ABI event data')
    a = 2+64*index
    if len(data) < a+64:
        raise DataError('Truncated ABI data')
    return int(data[a:a+64], 16)


def event_address(w: int) -> str:
    if w >> 160:
        raise DataError('Nonzero padding in ABI address')
    return addr('0x'+f'{w:040x}')


def int24(w: int) -> int:
    raw = w & ((1<<24)-1)
    result = raw-(1<<24) if raw & (1<<23) else raw
    expected = result % (1<<256)
    if w != expected:
        raise DataError('Invalid int24 sign extension')
    return result


def is_hash(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r'0x[0-9a-fA-F]{64}', value))


def decode_creation(log: dict) -> dict:
    """Strict ABIs, avoids silently turning malformed logs into discovered pools."""
    factory = addr(log['address'])
    topics = log['topics']
    if not isinstance(topics, list) or len(topics) != 4 or any(not is_hash(x) for x in topics):
        raise DataError('Unexpected log topics')
    if log.get('removed') is True:
        raise DataError('Removed log on confirmed-range query; fail closed')
    if not is_hash(log.get('transactionHash')) or not is_hash(log.get('blockHash')):
        raise DataError('Missing/invalid transaction or block hash')
    try:
        block = int(log['blockNumber'], 16)
        log_index = int(log['logIndex'], 16)
    except (ValueError, KeyError, TypeError) as e:
        raise DataError('Invalid block/log index') from e
    if block < 0 or log_index < 0:
        raise DataError('Negative block/log index')
    t0, t1 = event_address(int(topics[1],16)), event_address(int(topics[2],16))
    data = log.get('data', '')
    if factory == UNISWAP_V3_FACTORY and topics[0].lower() == UNI_TOPIC:
        if len(data) != 2+2*64:
            raise DataError('Uniswap V3 data length must be exactly 2 words')
        fee = int(topics[3],16)
        if fee > (1<<24)-1:
            raise DataError('Invalid uint24 fee')
        spacing = int24(word(data,0))
        pool = event_address(word(data,1))
        dex = 'uniswap_v3'
        extra = {'fee':fee,'tick_spacing':spacing}
    elif factory == AERODROME_CLASSIC_FACTORY and topics[0].lower() == AERO_TOPIC:
        if len(data) != 2+2*64:
            raise DataError('Aerodrome classic data length must be exactly 2 words')
        stable_word = int(topics[3],16)
        if stable_word not in (0,1):
            raise DataError('Invalid indexed bool stable')
        pool = event_address(word(data,0))
        index = word(data,1)
        dex = 'aerodrome_classic'
        extra = {'stable':bool(stable_word),'factory_pool_index':index}
    else:
        raise DataError('Unknown factory/topic pairing')
    if pool == '0x'+'0'*40 or t0 == t1:
        raise DataError('Impossible pool/token fields')
    return {'factory':factory,'dex':dex,'pool':pool,'token0':t0,'token1':t1,
            'extra':compact_json(extra),'block_number':block,
            'block_hash':log['blockHash'].lower(),'tx_hash':log['transactionHash'].lower(),
            'log_index':log_index}


def percentile(nums: list[float], pct: float) -> float | None:
    if not nums:
        return None
    nums = sorted(nums)
    ix = (len(nums)-1)*pct/100
    lo = int(ix)
    return round(nums[lo]+(nums[min(lo+1,len(nums)-1)]-nums[lo])*(ix-lo),3)


def config_dict(args: argparse.Namespace) -> dict:
    # Never include RPC key, path, or endpoint URL in configuration persisted to SQLite.
    return {key:getattr(args,key) for key in (
        'duration_hours','poll_seconds','confirmations','max_blocks_per_range',
        'max_ranges_per_cycle','max_active_pools','watch_minutes',
        'dex_requests_per_minute','gecko_requests_per_minute',
        'dex_spacing_seconds','gecko_spacing_seconds',
        'max_provider_calls_per_cycle','http_timeout','drain_minutes','no_dex','no_gecko')}


DDL = """
CREATE TABLE IF NOT EXISTS campaign(
 id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER NOT NULL,
 version TEXT NOT NULL, chain_id INTEGER NOT NULL, config_json TEXT NOT NULL,
 config_sha256 TEXT NOT NULL, started_ms INTEGER NOT NULL, deadline_ms INTEGER NOT NULL,
 start_block INTEGER NOT NULL, next_block INTEGER NOT NULL,
 end_target_block INTEGER, drain_deadline_ms INTEGER, completed_ms INTEGER,
 phase TEXT NOT NULL CHECK(phase IN ('running','draining','complete','incomplete_backlog'))
);
CREATE TABLE IF NOT EXISTS requests(
 id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
 operation TEXT NOT NULL, pool TEXT, started_ms INTEGER NOT NULL,
 ended_ms INTEGER NOT NULL, latency_ms REAL NOT NULL,
 success INTEGER NOT NULL, http_status INTEGER, error_kind TEXT, count_result INTEGER
);
CREATE INDEX IF NOT EXISTS requests_kind_time ON requests(kind,started_ms);
CREATE TABLE IF NOT EXISTS scanned_ranges(
 id INTEGER PRIMARY KEY, from_block INTEGER NOT NULL, to_block INTEGER NOT NULL,
 completed_ms INTEGER NOT NULL, event_count INTEGER NOT NULL,
 UNIQUE(from_block,to_block)
);
CREATE TABLE IF NOT EXISTS pools(
 pool TEXT PRIMARY KEY, dex TEXT NOT NULL, factory TEXT NOT NULL,
 token0 TEXT NOT NULL, token1 TEXT NOT NULL, extra_json TEXT NOT NULL,
 block_number INTEGER NOT NULL, block_hash TEXT NOT NULL,
 block_time_ms INTEGER NOT NULL, tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
 first_seen_ms INTEGER NOT NULL, admit_state TEXT NOT NULL,
 UNIQUE(tx_hash,log_index)
);
CREATE INDEX IF NOT EXISTS pools_admitted ON pools(admit_state,first_seen_ms);
CREATE TABLE IF NOT EXISTS provider_state(
 pool TEXT NOT NULL REFERENCES pools(pool), provider TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0, next_due_ms INTEGER NOT NULL,
 first_listed_ms INTEGER, first_priced_ms INTEGER, last_fingerprint TEXT,
 changed_count INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(pool,provider)
);
CREATE INDEX IF NOT EXISTS provider_due ON provider_state(provider,next_due_ms);
CREATE TABLE IF NOT EXISTS provider_checks(
 id INTEGER PRIMARY KEY AUTOINCREMENT, pool TEXT NOT NULL REFERENCES pools(pool),
 provider TEXT NOT NULL, request_id INTEGER REFERENCES requests(id),
 response_ms INTEGER NOT NULL, listed INTEGER NOT NULL, priced INTEGER NOT NULL,
 usd_price REAL, liquidity_usd REAL, provider_pair_created_ms INTEGER,
 changed_from_previous INTEGER, fingerprint TEXT, note TEXT
);
CREATE INDEX IF NOT EXISTS pc_pool_provider ON provider_checks(pool,provider,response_ms);
CREATE TABLE IF NOT EXISTS issues(
 id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, category TEXT NOT NULL,
 detail TEXT NOT NULL
);
"""


def db_connect(path: Path, read_only: bool=False) -> sqlite3.Connection:
    if read_only:
        if not path.is_file():
            raise DataError('Database does not exist: refused to create it')
        conn = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=8)
        conn.execute('PRAGMA query_only=ON')
    else:
        conn = sqlite3.connect(path, timeout=8)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=8000')
    return conn


def db_init(path: Path, args: argparse.Namespace, tip: int, started: int) -> sqlite3.Connection:
    if path.exists():
        raise DataError('Refusing existing database: use --resume, not a new campaign')
    if not path.parent.is_dir():
        raise DataError('Database parent directory must already exist')
    con = db_connect(path)
    try:
        con.executescript(DDL)
        settings = compact_json(config_dict(args))
        con.execute('INSERT INTO campaign VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (SCHEMA, VERSION,CHAIN_ID,settings,sha256(config_dict(args)),
                     started,started+round(args.duration_hours*3_600_000),
                     tip+1,tip+1,None,None,None,'running'))
        con.commit()
    except Exception:
        con.close()
        raise
    return con


def campaign(con: sqlite3.Connection) -> sqlite3.Row:
    con.row_factory = sqlite3.Row
    row = con.execute('SELECT * FROM campaign WHERE id=1').fetchone()
    if row is None or row['version'] != VERSION or row['schema_version'] != SCHEMA or row['chain_id'] != CHAIN_ID:
        raise DataError('Foreign/incompatible database or schema; refused')
    return row


class Transport:
    def __init__(self, rpc_url: str, timeout: float):
        u = urllib.parse.urlsplit(rpc_url)
        if u.scheme not in ('https','http') or not u.netloc or u.username or u.password:
            raise DataError('RPC URL requires http(s), no user-info; use provider token in path/query via environment')
        if u.scheme == 'http' and u.hostname not in ('127.0.0.1','localhost','::1'):
            raise DataError('Remote RPC must use HTTPS')
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.seq = 0

    def request(self, url: str, obj: dict | None=None, gecko: bool=False) -> tuple[Any,int]:
        if obj is None:
            req = urllib.request.Request(url, headers=GECKO_HEADERS if gecko else HEADERS)
        else:
            payload = compact_json(obj).encode('utf-8')
            req = urllib.request.Request(url, data=payload, method='POST',
                headers={**HEADERS,'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as reply:
                raw = reply.read(2_000_000+1)
                status = int(reply.status)
            if len(raw)>2_000_000:
                raise DataError('Oversized API response')
            return json.loads(raw), status
        except urllib.error.HTTPError as e:
            # Never echo raw response bodies: they may contain provider or account details.
            raise TransportError('http_'+str(e.code),e.code) from None
        except (urllib.error.URLError, TimeoutError) as e:
            raise TransportError(type(e).__name__) from None
        except (ValueError,UnicodeDecodeError) as e:
            raise TransportError('invalid_json') from None

    def rpc(self, method: str, params: list[Any]) -> tuple[Any,int]:
        if method not in ('eth_chainId','eth_blockNumber','eth_getLogs','eth_getBlockByNumber'):
            raise DataError('Non READ ONLY RPC method refused')
        self.seq+=1
        obj, status = self.request(self.rpc_url,{'jsonrpc':'2.0','id':self.seq,
                                                'method':method,'params':params})
        if not isinstance(obj,dict) or obj.get('jsonrpc') != '2.0' or obj.get('id') != self.seq:
            raise TransportError('invalid_rpc_envelope', status)
        if obj.get('error'):
            error = obj['error']
            code = error.get('code') if isinstance(error,dict) else None
            raise TransportError('rpc_error_'+str(code),status)
        if 'result' not in obj:
            raise TransportError('missing_rpc_result',status)
        return obj['result'], status


class TransportError(RuntimeError):
    def __init__(self, kind: str, status: int | None=None):
        super().__init__(kind)
        self.kind=kind
        self.status=status


def recorded_call(con: sqlite3.Connection, transport: Transport, kind: str,
                  operation: str, pool: str | None=None,
                  params: list[Any] | None=None) -> tuple[Any,int,int]:
    began=now_ms()
    tick=time.monotonic()
    data,status,err = None,None,None
    try:
        if kind=='rpc':
            data,status=transport.rpc(operation,params or [])
        elif kind=='dex':
            data,status=transport.request(DEX_URL+pool)
        elif kind=='gecko':
            data,status=transport.request(GECKO_URL+pool,gecko=True)
        else:
            raise DataError('Unknown source')
        return_data=True
    except TransportError as ex:
        err=ex.kind
        status=ex.status
        return_data=False
    except (OSError,DataError) as ex:
        err=type(ex).__name__
        return_data=False
    done=now_ms()
    elapsed=round((time.monotonic()-tick)*1000,3)
    result_count=len(data) if isinstance(data,list) else None
    cur=con.execute('INSERT INTO requests(kind,operation,pool,started_ms,ended_ms,latency_ms,success,http_status,error_kind,count_result) '
                    'VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (kind,operation,pool,began,done,elapsed,int(return_data),status,err,result_count))
    con.commit()
    if not return_data:
        raise TransportError(err or 'unknown',status)
    return data,done,cur.lastrowid


def get_tip(con: sqlite3.Connection, transport: Transport) -> int:
    result,_,_=recorded_call(con,transport,'rpc','eth_blockNumber',params=[])
    try:
        v=int(result,16)
        if v<0: raise ValueError()
        return v
    except (ValueError, TypeError):
        raise DataError('Invalid block number') from None


def get_block(con: sqlite3.Connection, transport: Transport, number: int) -> tuple[int,str]:
    block,_,_=recorded_call(con,transport,'rpc','eth_getBlockByNumber',params=[hex(number),False])
    if not isinstance(block,dict) or not is_hash(block.get('hash')):
        raise DataError('Invalid block data/hash')
    if int(block['number'],16) != number:
        raise DataError('Block number mismatch')
    sec=int(block['timestamp'],16)
    return sec*1000,block['hash'].lower()


def persist_range(con: sqlite3.Connection, decoded: list[dict], first: int, last: int,
                  received: int, watch_ms: int, max_active: int) -> tuple[int,int]:
    """Atomic cursor advancement: one SQL transaction per fully decoded range."""
    row=campaign(con)
    if row['next_block'] != first:
        raise DataError('Noncontiguous cursor; fail closed')
    if last < first:
        raise DataError('Invalid range')
    newly, excluded=0,0
    try:
        con.execute('BEGIN IMMEDIATE')
        con.execute("UPDATE pools SET admit_state='expired' WHERE admit_state='tracked' AND first_seen_ms+?<=?",
                    (watch_ms,received))
        active=con.execute("SELECT COUNT(*) FROM pools WHERE admit_state='tracked'").fetchone()[0]
        for e in sorted(decoded,key=lambda x:(x['block_number'],x['log_index'])):
            if not first<=e['block_number']<=last:
                raise DataError('Log outside requested block range')
            prior=con.execute('SELECT tx_hash, log_index, block_hash FROM pools WHERE pool=?',
                              (e['pool'],)).fetchone()
            if prior:
                if (prior['tx_hash'],prior['log_index'],prior['block_hash'])!=(e['tx_hash'],e['log_index'],e['block_hash']):
                    raise DataError('Pool/event conflict, possible reorg')
                continue
            is_early = row['phase']=='running' and received < row['deadline_ms']
            is_admitted = is_early and active < max_active
            state = ('tracked' if is_admitted else
                     ('not_admitted_capacity' if is_early else 'recovered_after_cutoff'))
            con.execute('INSERT INTO pools VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (e['pool'],e['dex'],e['factory'],e['token0'],e['token1'],e['extra'],
                         e['block_number'],e['block_hash'],e['block_time_ms'],
                         e['tx_hash'],e['log_index'],received,state))
            if is_admitted:
                active+=1
                for p in ('dex','gecko'):
                    con.execute('INSERT INTO provider_state(pool,provider,next_due_ms) VALUES(?,?,?)',
                                (e['pool'],p,received))
                newly+=1
            else:
                excluded+=1
        con.execute('INSERT INTO scanned_ranges(from_block,to_block,completed_ms,event_count) VALUES(?,?,?,?)',
                    (first,last,received,len(decoded)))
        con.execute('UPDATE campaign SET next_block=? WHERE id=1',(last+1,))
        con.commit()
    except Exception:
        con.rollback()
        raise
    return newly,excluded


def scan_ranges(con: sqlite3.Connection, transport: Transport, tip: int,
                args: argparse.Namespace, target: int | None=None) -> tuple[int,int,int]:
    safe=tip-args.confirmations
    if target is not None:
        safe=min(safe,target)
    events,admitted,excluded=0,0,0
    for _ in range(args.max_ranges_per_cycle):
        cur=campaign(con)['next_block']
        if cur>safe:
            break
        end=min(safe,cur+args.max_blocks_per_range-1)
        params=[{'fromBlock':hex(cur),'toBlock':hex(end),
                 'address':[UNISWAP_V3_FACTORY,AERODROME_CLASSIC_FACTORY],
                 'topics':[[UNI_TOPIC,AERO_TOPIC]]}]
        try:
            logs,received,_=recorded_call(con,transport,'rpc','eth_getLogs',params=params)
            if not isinstance(logs,list):
                raise DataError('RPC getLogs returned non-list')
            decoded=[decode_creation(x) for x in logs]
            block_cache={}
            for e in decoded:
                num=e['block_number']
                if num not in block_cache:
                    block_cache[num]=get_block(con,transport,num)
                e['block_time_ms'],canonical_hash=block_cache[num]
                if canonical_hash !=e['block_hash']:
                    raise DataError('Block hash differs from receipt; possible reorg')
            n,k=persist_range(con,decoded,cur,end,received,
                              args.watch_minutes*60_000,args.max_active_pools)
            events+=len(decoded);admitted+=n;excluded+=k
        except (TransportError,DataError,sqlite3.Error) as ex:
            # Cursor is unchanged on failure. Persist only a sanitized category.
            con.execute('INSERT INTO issues(at_ms,category,detail) VALUES(?,?,?)',
                        (now_ms(),'scan_error',type(ex).__name__ if not isinstance(ex,TransportError) else ex.kind))
            con.commit()
            print('SCAN WARNING: cursor unchanged; '+type(ex).__name__,flush=True)
            break
    return events,admitted,excluded


def finite_nonnegative(x: Any) -> float | None:
    try:
        if x is None or x=='':
            return None
        f=float(x)
        return f if math.isfinite(f) and f>=0 else None
    except (ValueError,TypeError,OverflowError):
        return None


def pair_fields(provider: str, data: Any, target: str) -> dict:
    """A missing/unpriced listing is not automatically a failed trade or fraud."""
    result={'listed':False,'priced':False,'price':None,'liq':None,
            'provider_pair_created_ms':None,'fingerprint':None,'note':'not_listed'}
    pair=None
    if provider=='dex':
        if not isinstance(data,dict):
            raise DataError('DexScreener payload is not object')
        found=data.get('pairs')
        if found is not None and not isinstance(found,list):
            raise DataError('Unexpected DexScreener pairs')
        for candidate in found or []:
            if isinstance(candidate,dict) and str(candidate.get('chainId','')).lower()=='base' and \
                    str(candidate.get('pairAddress','')).lower()==target:
                pair=candidate
                break
        if pair is not None:
            result['price']=finite_nonnegative(pair.get('priceUsd'))
            liquidity=pair.get('liquidity')
            result['liq']=finite_nonnegative(liquidity.get('usd') if isinstance(liquidity,dict) else None)
            result['provider_pair_created_ms']=finite_nonnegative(pair.get('pairCreatedAt'))
            relevant={key:pair.get(key) for key in ('priceUsd','liquidity','volume','txns','priceChange')}
            result['fingerprint']=sha256(relevant)
    elif provider=='gecko':
        if not isinstance(data,dict):
            raise DataError('Gecko payload is not object')
        val=data.get('data')
        if isinstance(val,dict):
            attributes=val.get('attributes')
            if isinstance(attributes,dict) and \
                    str(attributes.get('address','')).lower()==target and \
                    str(val.get('id','')).lower()=='base_'+target:
                pair=attributes
        if pair is not None:
            result['price']=finite_nonnegative(pair.get('base_token_price_usd'))
            result['liq']=finite_nonnegative(pair.get('reserve_in_usd'))
            relevant={key:pair.get(key) for key in ('base_token_price_usd','reserve_in_usd',
                       'volume_usd','transactions','price_change_percentage')}
            result['fingerprint']=sha256(relevant)
    else:
        raise DataError('Unknown provider')
    if pair is not None:
        result['listed']=True
        result['priced']=result['price'] is not None and result['price']>0
        result['note']='priced' if result['priced'] else 'listed_unpriced'
    return result


def provider_budget_available(con: sqlite3.Connection, source: str,
                              now: int, limit: int) -> bool:
    count=con.execute('SELECT COUNT(*) FROM requests WHERE kind=? AND started_ms>?',
                      (source,now-60_000)).fetchone()[0]
    return count < limit


def poll_provider(con: sqlite3.Connection, transport: Transport, source: str,
                  now: int, args: argparse.Namespace) -> int:
    if (source=='dex' and args.no_dex) or (source=='gecko' and args.no_gecko):
        return 0
    limit = args.dex_requests_per_minute if source=='dex' else args.gecko_requests_per_minute
    if limit==0:
        return 0
    spacing=(args.dex_spacing_seconds if source=='dex' else args.gecko_spacing_seconds)*1000
    limit_calls=args.max_provider_calls_per_cycle
    used=0
    while used<limit_calls and provider_budget_available(con,source,now_ms(),limit):
        row=con.execute("""
            SELECT ps.pool,ps.attempts,ps.last_fingerprint,ps.first_listed_ms,ps.first_priced_ms
            FROM provider_state ps JOIN pools p ON p.pool=ps.pool
            WHERE ps.provider=? AND p.admit_state='tracked'
              AND p.first_seen_ms+?>? AND ps.next_due_ms<=?
            ORDER BY ps.next_due_ms, p.first_seen_ms,ps.pool LIMIT 1
            """,(source,args.watch_minutes*60_000,now_ms(),now_ms())).fetchone()
        if not row:
            break
        pool=row['pool']
        outcome={'listed':False,'priced':False,'price':None,'liq':None,
                 'provider_pair_created_ms':None,'fingerprint':None,'note':'request_failed'}
        request_id=None
        try:
            data,received,request_id=recorded_call(con,transport,source,'pool_lookup',pool=pool)
            outcome=pair_fields(source,data,pool)
        except (TransportError,DataError) as ex:
            received=now_ms()
            # A syntactically invalid provider payload is not "not listed".
            outcome['note']= 'error_'+(ex.kind if isinstance(ex,TransportError) else 'invalid_payload')
        change=None
        if outcome['fingerprint'] is not None and row['last_fingerprint'] is not None:
            change=int(outcome['fingerprint'] !=row['last_fingerprint'])
        with con:
            con.execute("""
              INSERT INTO provider_checks(pool,provider,request_id,response_ms,listed,priced,
                  usd_price,liquidity_usd,provider_pair_created_ms,changed_from_previous,fingerprint,note)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
             (pool,source,request_id,received,int(outcome['listed']),int(outcome['priced']),
              outcome['price'],outcome['liq'],outcome['provider_pair_created_ms'],
              change,outcome['fingerprint'],outcome['note']))
            con.execute("""
              UPDATE provider_state SET attempts=attempts+1, next_due_ms=?,
              first_listed_ms=CASE WHEN first_listed_ms IS NULL AND ? THEN ? ELSE first_listed_ms END,
              first_priced_ms=CASE WHEN first_priced_ms IS NULL AND ? THEN ? ELSE first_priced_ms END,
              last_fingerprint=COALESCE(?,last_fingerprint),
              changed_count=changed_count+?
              WHERE pool=? AND provider=?""",
             (received+spacing,int(outcome['listed']),received,int(outcome['priced']),received,
              outcome['fingerprint'],int(change or 0),pool,source))
        used+=1
    return used


def init_live(args: argparse.Namespace, transport: Transport) -> sqlite3.Connection:
    # Do not create SQLite until chain validation and live starting block are known.
    if args.resume:
        con=db_connect(Path(args.db)) if Path(args.db).exists() else None
        if con is None:
            raise DataError('Resume requested but database does not exist')
        row=campaign(con)
        if row['config_sha256']!=sha256(config_dict(args)):
            con.close()
            raise DataError('Resume settings mismatch; refused')
        if row['phase'] in ('complete','incomplete_backlog'):
            con.close()
            raise DataError('Completed/incomplete campaign is immutable; no resume')
    else:
        if Path(args.db).exists():
            raise DataError('Fresh experiment requires a new database filename')
        con=None
    try:
        # chain check no writes / no key, wallet or transaction methods
        raw,_=transport.rpc('eth_chainId',[])
        if int(raw,16)!=CHAIN_ID:
            raise DataError('RPC chainId mismatch: expected Base 8453')
        if con is None:
            tip_raw,_=transport.rpc('eth_blockNumber',[])
            tip=int(tip_raw,16)
            con=db_init(Path(args.db),args,tip,now_ms())
        return con
    except Exception:
        if con is not None:
            con.close()
        raise


def mark_phase(con: sqlite3.Connection, phase: str, **fields: int | None) -> None:
    cols=['phase=?']
    vals=[phase]
    for k,v in fields.items():
        if k not in ('end_target_block','drain_deadline_ms','completed_ms'):
            raise DataError('Invalid campaign column')
        cols.append(k+'=?');vals.append(v)
    con.execute('UPDATE campaign SET '+','.join(cols)+' WHERE id=1',(*vals,))
    con.commit()


def expire_watches(con: sqlite3.Connection, now: int, watch_ms: int) -> None:
    con.execute("UPDATE pools SET admit_state='expired' WHERE admit_state='tracked' AND first_seen_ms+?<=?",
                (watch_ms,now))
    con.commit()


def run(args: argparse.Namespace, transport: Transport | None=None) -> int:
    tr=transport or Transport(args.rpc_url,args.http_timeout)
    con=init_live(args,tr)
    try:
        row=campaign(con)
        print(f'DEX EARLY FEASIBILITY V{VERSION} READ ONLY | chain=8453 | db={args.db} '
              f'| start_block={row["start_block"]} | deadline_utc_ms={row["deadline_ms"]} '
              f'| uniswap_topic={UNI_TOPIC} | aerodrome_topic={AERO_TOPIC}',flush=True)
        loops=0
        while True:
            loops+=1
            now=now_ms()
            expire_watches(con,now,args.watch_minutes*60_000)
            row=campaign(con)
            if row['phase']=='running' and now>=row['deadline_ms']:
                # Freeze a block bound near the cutoff, then drain already confirmed ranges.
                try:
                    tip=get_tip(con,tr)
                    bound=max(row['start_block']-1,tip-args.confirmations)
                    con.execute("UPDATE pools SET admit_state='censored_at_campaign_end' WHERE admit_state='tracked'")
                    con.commit()
                    mark_phase(con,'draining',end_target_block=bound,
                               drain_deadline_ms=now+args.drain_minutes*60_000)
                except (TransportError,DataError) as ex:
                    print('CUTOFF WARNING: cannot freeze target block; retry',flush=True)
                    if loops==1 and args.once: break
                    time.sleep(args.poll_seconds)
                    continue
            row=campaign(con)
            try:
                tip=get_tip(con,tr)
                target=row['end_target_block'] if row['phase']=='draining' else None
                observed,admitted,excluded=scan_ranges(con,tr,tip,args,target)
            except (TransportError,DataError) as ex:
                observed=admitted=excluded=0
                print('RPC WARNING: '+type(ex).__name__+'; no cursor advancement',flush=True)
            if row['phase']=='running' and now_ms()<row['deadline_ms']:
                dex=poll_provider(con,tr,'dex',now_ms(),args)
                gecko=poll_provider(con,tr,'gecko',now_ms(),args)
            else:
                dex=gecko=0
            row=campaign(con)
            if row['phase']=='draining':
                if row['next_block']>row['end_target_block']:
                    mark_phase(con,'complete',completed_ms=now_ms())
                elif now_ms()>=row['drain_deadline_ms']:
                    mark_phase(con,'incomplete_backlog',completed_ms=now_ms())
            print(f'loop={loops} phase={campaign(con)["phase"]} next_block={campaign(con)["next_block"]} '
                  f'events_new={observed} tracked_new={admitted} not_admitted_new={excluded} '
                  f'dex_checks={dex} gecko_checks={gecko}',flush=True)
            if campaign(con)['phase'] in ('complete','incomplete_backlog') or args.once:
                break
            time.sleep(args.poll_seconds)
        print('Final/current report:',compact_json(report(con)),flush=True)
        return 0 if campaign(con)['phase']!='incomplete_backlog' else 2
    finally:
        con.close()


def report(con: sqlite3.Connection) -> dict:
    r=campaign(con)
    result={
     'version':VERSION,'chain_id':CHAIN_ID,
     'campaign':{k:r[k] for k in ('phase','started_ms','deadline_ms','start_block',
                                   'next_block','end_target_block','completed_ms')},
     'factory_scope':['uniswap_v3','aerodrome_classic'],
     'methods_issued_read_only':{},
     'events_per_factory':{},
     'admission_states':{},
     'provider':{},
     'scanned_ranges':con.execute('SELECT COUNT(*) FROM scanned_ranges').fetchone()[0],
     'scanned_block_count':con.execute('SELECT COALESCE(SUM(to_block-from_block+1),0) FROM scanned_ranges').fetchone()[0],
     'event_to_vps_lag_includes_confirmations_and_block_timestamp_resolution':{},
     'issues':{},
     'interpretation':['Only 2 factories on Base; no all-chain census',
        'Event detection timestamp is receipt of confirmed eth_getLogs response, not mempool time',
        'Provider first-seen is an upper bound on listing time; absence may mean no polling',
        'No price path, tradeability or trading PnL can be inferred'],
    }
    for kind,n in con.execute('SELECT operation,COUNT(*) FROM requests WHERE kind="rpc" GROUP BY operation'):
        result['methods_issued_read_only'][kind]=n
    for factory,n in con.execute('SELECT dex,COUNT(*) FROM pools GROUP BY dex'):
        result['events_per_factory'][factory]=n
    for state,n in con.execute('SELECT admit_state,COUNT(*) FROM pools GROUP BY admit_state'):
        result['admission_states'][state]=n
    delays=[(seen-blockts)/1000 for seen,blockts in
      con.execute('SELECT first_seen_ms,block_time_ms FROM pools')]
    result['event_to_vps_lag_includes_confirmations_and_block_timestamp_resolution']={
       'n':len(delays),'p50_s':percentile(delays,50),'p95_s':percentile(delays,95),
       'negative_clock_skew_count':sum(x<0 for x in delays)}
    for source in ('dex','gecko'):
        rows=list(con.execute("""SELECT p.first_seen_ms,ps.attempts,ps.first_listed_ms,
           ps.first_priced_ms,ps.changed_count
           FROM provider_state ps JOIN pools p ON p.pool=ps.pool WHERE ps.provider=?""",(source,)))
        vals=[(lst-seen)/1000 for seen,attempt,lst,priced,ch in rows if lst is not None]
        priced=[(px-seen)/1000 for seen,attempt,lst,px,ch in rows if px is not None]
        result['provider'][source]={
          'admitted_pool_count':len(rows),
          'never_polled_count':sum(x[1]==0 for x in rows),
          'polled_but_never_listed_count':sum(x[1]>0 and x[2] is None for x in rows),
          'first_listed_count':len(vals),'first_priced_count':len(priced),
          'listing_delay_p50_s_since_vps_event':percentile(vals,50),
          'listing_delay_p95_s_since_vps_event':percentile(vals,95),
          'priced_delay_p50_s_since_vps_event':percentile(priced,50),
          'priced_delay_p95_s_since_vps_event':percentile(priced,95),
          'changed_payload_count':sum(x[4] for x in rows),
          'requests_total':con.execute('SELECT COUNT(*) FROM requests WHERE kind=?',(source,)).fetchone()[0],
          'requests_errors':con.execute('SELECT COUNT(*) FROM requests WHERE kind=? AND success=0',(source,)).fetchone()[0],
        }
    for key,n in con.execute('SELECT category,COUNT(*) FROM issues GROUP BY category'):
        result['issues'][key]=n
    rpcs=con.execute('SELECT COUNT(*),SUM(success=0) FROM requests WHERE kind="rpc"').fetchone()
    result['rpc_requests']=rpcs[0]
    result['rpc_errors']=rpcs[1] or 0
    return result


class Tests(unittest.TestCase):
    def mkargs(self,**kw):
        a=parse_args(['--db','unused.sqlite3'])
        for k,v in kw.items():setattr(a,k,v)
        return a

    def test_keccak_vectors(self):
        self.assertEqual(keccak256(b'').hex(),
         'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470')
        self.assertTrue(AERO_TOPIC.startswith('0x'))

    def test_topics_distinct(self):
        self.assertNotEqual(UNI_TOPIC,AERO_TOPIC)
        self.assertEqual(len(UNI_TOPIC),66)

    def make_log(self,aero=False,block=10):
        def w(n): return '0x'+f'{n:064x}'
        address=AERODROME_CLASSIC_FACTORY if aero else UNISWAP_V3_FACTORY
        data='0x'+f'{999:064x}'+f'{567:064x}' if aero else \
             '0x'+f'{60:064x}'+f'{999:064x}'
        return dict(address=address,topics=[AERO_TOPIC if aero else UNI_TOPIC,
             w(111),w(222),w(1 if aero else 3000)],data=data,
             transactionHash=w(1234),blockHash=w(4567),blockNumber=hex(block),
             logIndex='0x0',removed=False)

    def test_decode_uniswap(self):
        e=decode_creation(self.make_log())
        self.assertEqual(json.loads(e['extra'])['fee'],3000)
        self.assertEqual(e['pool'], '0x'+f'{999:040x}')
        self.assertEqual(e['dex'],'uniswap_v3')

    def test_decode_aero(self):
        e=decode_creation(self.make_log(aero=True))
        self.assertEqual(e['dex'],'aerodrome_classic')
        self.assertTrue(json.loads(e['extra'])['stable'])
        self.assertEqual(json.loads(e['extra'])['factory_pool_index'],567)

    def test_wrong_topic_factory_rejected(self):
        x=self.make_log(aero=True)
        x['address']=UNISWAP_V3_FACTORY
        with self.assertRaises(DataError):decode_creation(x)

    def test_malformed_log_rejected(self):
        x=self.make_log();x['data']='0x1234'
        with self.assertRaises(DataError):decode_creation(x)

    def test_removed_log_rejected(self):
        x=self.make_log();x['removed']=True
        with self.assertRaises(DataError):decode_creation(x)

    def test_readonly_transport_whitelist(self):
        t=Transport(RPC_PUBLIC,1)
        with self.assertRaises(DataError):t.rpc('eth_sendRawTransaction',[])

    def test_remote_http_refused(self):
        with self.assertRaises(DataError): Transport('http://example.com/rpc',4)

    def test_pool_exact_matching_dex(self):
        target='0x'+'a'*40
        x=pair_fields('dex',{'pairs':[{'chainId':'base','pairAddress':target,
             'priceUsd':'1.5','liquidity':{'usd':20000},'volume':{'m5':10}}]},target)
        self.assertTrue(x['priced'])
        self.assertEqual(x['liq'],20000)
        self.assertFalse(pair_fields('dex',{'pairs':[{'chainId':'ethereum','pairAddress':target}]},target)['listed'])

    def test_pool_exact_matching_gecko(self):
        target='0x'+'a'*40
        x=pair_fields('gecko',{'data':{'id':'base_'+target,
             'attributes':{'address':target,'base_token_price_usd':'0.02'}}},target)
        self.assertTrue(x['priced'])

    def test_missing_price_not_zero(self):
        target='0x'+'a'*40
        x=pair_fields('dex',{'pairs':[{'chainId':'base','pairAddress':target,'priceUsd':None}]},target)
        self.assertTrue(x['listed']);self.assertFalse(x['priced']);self.assertIsNone(x['price'])

    def init_fixture(self,p,a=None):
        args=a or self.mkargs(db=str(p))
        return db_init(p,args,tip=9,started=1_000_000)

    def test_fresh_db_resume_protection(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3'
            con=self.init_fixture(p);con.close()
            with self.assertRaises(DataError):self.init_fixture(p)
            ro=db_connect(p,True)
            with self.assertRaises(sqlite3.OperationalError):ro.execute('DELETE FROM campaign')
            ro.close()

    def test_atomic_cursor_no_advancement_on_invalid_range(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            e=decode_creation(self.make_log(block=11));e['block_time_ms']=1_000_000
            with self.assertRaises(DataError):persist_range(con,[e],10,10,1_001_000,60_000,1)
            self.assertEqual(campaign(con)['next_block'],10)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM pools').fetchone()[0],0)
            con.close()

    def test_capacity_exclusion_preserved(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            e1=decode_creation(self.make_log(block=10));e1['block_time_ms']=1_000_000
            x=self.make_log(aero=True,block=10);x['transactionHash']='0x'+f'{9999:064x}'
            x['data']='0x'+f'{998:064x}'+f'{567:064x}'
            e2=decode_creation(x);e2['block_time_ms']=1_000_000
            n,k=persist_range(con,[e1,e2],10,10,1_001_000,120_000,1)
            self.assertEqual((n,k),(1,1))
            self.assertEqual(report(con)['admission_states']['not_admitted_capacity'],1)
            self.assertEqual(campaign(con)['next_block'],11)
            con.close()

    def test_recovered_after_cutoff_not_prospective(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            mark_phase(con,'draining',end_target_block=10,drain_deadline_ms=2_000_000)
            e=decode_creation(self.make_log());e['block_time_ms']=1_000_000
            n,k=persist_range(con,[e],10,10,1_001_000,120_000,10)
            self.assertEqual((n,k),(0,1))
            self.assertEqual(report(con)['admission_states']['recovered_after_cutoff'],1)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM provider_state').fetchone()[0],0)
            con.close()

    def test_received_after_deadline_not_prospective(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            e=decode_creation(self.make_log());e['block_time_ms']=1_000_000
            n,k=persist_range(con,[e],10,10,1_000_000+6*3_600_000+1,120_000,10)
            self.assertEqual((n,k),(0,1))
            con.close()

    def test_cannot_skip_ranges(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            with self.assertRaises(DataError):persist_range(con,[],11,12,1_001_000,60_000,2)
            con.close()

    def test_empty_ranges_count_as_scanned(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            persist_range(con,[],10,13,1_001_000,60_000,2)
            r=report(con)
            self.assertEqual(r['scanned_block_count'],4)
            self.assertEqual(campaign(con)['next_block'],14)
            con.close()

    def test_provider_budget_persists_across_restart(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            con.execute("INSERT INTO requests(kind,operation,started_ms,ended_ms,latency_ms,success) VALUES('dex','pool_lookup',?,?,?,1)",
                        (10_000,10_050,50))
            con.commit()
            self.assertFalse(provider_budget_available(con,'dex',10_100,1))
            self.assertTrue(provider_budget_available(con,'dex',71_000,1))
            con.close()

    def test_report_does_not_mutate_db(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p);con.close()
            before=p.read_bytes()
            con=db_connect(p,True);report(con);con.close()
            self.assertEqual(before,p.read_bytes())

    def test_end_to_end_mock_rpc_and_provider_resume(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'trial.sqlite3'
            log=self.make_log(block=101)
            log['blockHash']='0x'+f'{10101:064x}'
            pool='0x'+f'{999:040x}'
            clock=now_ms()
            class FakeTransport:
                def __init__(self): self.head_calls=0
                def rpc(self,method,params):
                    if method=='eth_chainId':return hex(CHAIN_ID),200
                    if method=='eth_blockNumber':
                        self.head_calls+=1
                        return ('0x64' if self.head_calls==1 else '0x69'),200  # 100 -> 105
                    if method=='eth_getLogs':
                        x=params[0]
                        self_outer.assertEqual(x['address'],[UNISWAP_V3_FACTORY,AERODROME_CLASSIC_FACTORY])
                        self_outer.assertIn(UNI_TOPIC,x['topics'][0])
                        return ([log] if int(x['fromBlock'],16)<=101<=int(x['toBlock'],16) else []),200
                    if method=='eth_getBlockByNumber':
                        return {'number':hex(101),'timestamp':hex((clock-3000)//1000),
                                'hash':log['blockHash']},200
                    raise AssertionError('unexpected method: '+method)
                def request(self,url,gecko=False):
                    if gecko:
                        return {'data':{'id':'base_'+pool,'attributes':{
                          'address':pool,'base_token_price_usd':'2.2',
                          'reserve_in_usd':'44000'}}},200
                    return {'pairs':[{'chainId':'base','pairAddress':pool,
                                     'priceUsd':'2.0','liquidity':{'usd':40000}}]},200
            self_outer=self
            a=self.mkargs(db=str(p),max_provider_calls_per_cycle=1,
                max_ranges_per_cycle=1,duration_hours=1,once=True)
            self.assertEqual(run(a,FakeTransport()),0)
            ro=db_connect(p,read_only=True)
            r=report(ro)
            self.assertEqual(r['events_per_factory']['uniswap_v3'],1)
            self.assertEqual(r['provider']['dex']['first_priced_count'],1)
            self.assertEqual(r['provider']['gecko']['first_priced_count'],1)
            self.assertEqual(r['scanned_block_count'],2)  # 101,102
            self.assertEqual(r['rpc_errors'],0)
            ro.close()
            a.resume=True
            self.assertEqual(run(a,FakeTransport()),0)
            ro=db_connect(p,read_only=True)
            self.assertEqual(sum(report(ro)['events_per_factory'].values()),1)
            ro.close()

    def test_corrupted_ethereum_log_does_not_advance_cursor(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'trial.sqlite3';a=self.mkargs()
            con=db_init(p,a,tip=9,started=now_ms())
            bad=self.make_log(block=10);bad['data']='0x123'
            class Fake:
                def rpc(self,method,params):
                    self_outer.assertEqual(method,'eth_getLogs')
                    return [bad],200
            self_outer=self
            scan_ranges(con,Fake(),tip=15,args=a)
            self.assertEqual(campaign(con)['next_block'],10)
            self.assertEqual(report(con)['issues']['scan_error'],1)
            con.close()

    def test_no_baseline_or_trade_field(self):
        with TemporaryDirectory() as d:
            p=Path(d)/'d.sqlite3';con=self.init_fixture(p)
            keys=list(report(con))
            self.assertNotIn('expectancy',keys)
            con.close()


def validate_args(a: argparse.Namespace) -> None:
    if not (0<a.duration_hours<=168):raise DataError('Duration hours must be in (0,168]')
    if not (2<=a.poll_seconds<=300):raise DataError('Polling seconds must be in [2,300]')
    if not (1<=a.confirmations<=100):raise DataError('Confirmations must be in [1,100]')
    if not (1<=a.max_blocks_per_range<=1000):raise DataError('Block range must be 1..1000')
    if not (1<=a.max_ranges_per_cycle<=20):raise DataError('Max ranges/cycle must be 1..20')
    if not (1<=a.max_active_pools<=500):raise DataError('Max active pools 1..500')
    if not (1<=a.watch_minutes<=1440):raise DataError('Watch minutes 1..1440')
    if not (0<=a.dex_requests_per_minute<=60):raise DataError('Dex rate must be 0..60')
    if not (0<=a.gecko_requests_per_minute<=10):raise DataError('Gecko rate must be 0..10')
    if not (10<=a.dex_spacing_seconds<=3600):raise DataError('Dex spacing 10..3600')
    if not (10<=a.gecko_spacing_seconds<=3600):raise DataError('Gecko spacing 10..3600')
    if not (1<=a.max_provider_calls_per_cycle<=20):raise DataError('Max provider calls/cycle 1..20')
    if not (2<=a.http_timeout<=60):raise DataError('HTTP timeout 2..60')
    if not (1<=a.drain_minutes<=120):raise DataError('Drain minutes 1..120')


def parse_args(argv: list[str] | None=None) -> argparse.Namespace:
    p=argparse.ArgumentParser(description='Base on-chain early-discovery feasibility, READ ONLY (no trading)')
    p.add_argument('--db',default='data/dex_early_feasibility_v01.sqlite3')
    p.add_argument('--rpc-url',default=os.getenv('BASE_RPC_URL',RPC_PUBLIC),
                   help='HTTPS Base RPC endpoint (prefer env BASE_RPC_URL; never print secrets)')
    p.add_argument('--duration-hours',type=float,default=6)
    p.add_argument('--poll-seconds',type=float,default=10)
    p.add_argument('--confirmations',type=int,default=3)
    p.add_argument('--max-blocks-per-range',type=int,default=40)
    p.add_argument('--max-ranges-per-cycle',type=int,default=3)
    p.add_argument('--max-active-pools',type=int,default=12)
    p.add_argument('--watch-minutes',type=int,default=30)
    p.add_argument('--dex-requests-per-minute',type=int,default=12)
    p.add_argument('--gecko-requests-per-minute',type=int,default=2)
    p.add_argument('--dex-spacing-seconds',type=int,default=60)
    p.add_argument('--gecko-spacing-seconds',type=int,default=120)
    p.add_argument('--max-provider-calls-per-cycle',type=int,default=3)
    p.add_argument('--http-timeout',type=float,default=10)
    p.add_argument('--drain-minutes',type=int,default=10)
    p.add_argument('--no-dex',action='store_true')
    p.add_argument('--no-gecko',action='store_true')
    p.add_argument('--resume',action='store_true',help='Resume same DB and frozen configuration')
    p.add_argument('--once',action='store_true',help='Run one loop; leaves campaign active for --resume')
    p.add_argument('--report',action='store_true',help='Only read existing database; never call APIs')
    p.add_argument('--self-test',action='store_true')
    return p.parse_args(argv)


def main(argv: list[str] | None=None) -> int:
    a=parse_args(argv)
    if a.self_test:
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        result=unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if a.report:
        con=db_connect(Path(a.db),read_only=True)
        try:print(json.dumps(report(con),indent=2))
        finally:con.close()
        return 0
    validate_args(a)
    return run(a)


if __name__=='__main__':
    try:
        sys.exit(main())
    except (DataError,TransportError,sqlite3.Error,OSError,ValueError) as ex:
        # No raw URLs, URLs can contain provider API keys.
        print('ERROR: '+type(ex).__name__+': '+(str(ex) if isinstance(ex,DataError)
                 and not ('http' in str(ex).lower() or 'url' in str(ex).lower()) else 'operation failed'),file=sys.stderr)
        sys.exit(2)
