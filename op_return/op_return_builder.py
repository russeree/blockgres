import os
import sys
import queue
import threading
import psycopg2
import colorama
import time
import codecs
import copy

from colorama import Fore, Back, Style
from psycopg2 import pool
from dotenv import load_dotenv
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

load_dotenv()

class OpReturnPGSQL:
    def __init__(self, btcRpcWorkers = 2):
        print("bitcoin core OP_RETURN script_pub_key to postgesql v0.0.1")
        #public vars
        self.btcRpcWorkers = btcRpcWorkers;
        self.pgPool = psycopg2.pool.ThreadedConnectionPool(1,500,
            user = os.environ.get('PGSQL_USER'),
            password = os.environ.get('PGSQL_PASS'),
            host = os.environ.get('PGSQL_HOST'),
            port = os.environ.get('PGSQL_PORT'),
            database = os.environ.get('PGSQL_DB'))
        self.btcRpc = AuthServiceProxy("http://%s:%s@%s:%s"%(
            os.environ.get('BTCRPC_USER'),
            os.environ.get('BTCRPC_PASS'),
            os.environ.get('BTCRPC_HOST'),
            os.environ.get('BTCRPC_PORT')),
            timeout = 1000)
        self.getblockQ = queue.Queue()
        # self.pgWriteBlocksToTip()
        self.isStandardOpReturnToText()

    ##
    #Deconstructor
    def __del__(self):
        print("cleaning up OpReturnPGSQL module")
    ##
    #Create a list of pure OP_RETURNS Scripts
    def isStandardOpReturnToText(self):
        try:
            conn = self.pgPool.getconn()
            _queryText = f'SELECT script_pubkey, row_hash, txid FROM op_return_onchain'
            _queryVars = []
            if(conn):
                cursor = conn.cursor()
                cursor.execute(_queryText,_queryVars)
                for row in cursor.fetchall():
                    if not row[0].find('OP_RETURN '):
                        opreturnText = row[0].replace('OP_RETURN ','').split()
                        if len(opreturnText) == 1:
                            if not len(opreturnText[0])%2:
                                try:
                                    utf8 = bytes.fromhex(opreturnText[0]).decode('utf-8')
                                    if len(utf8.lower()) > 30:
                                        print(f'TXID:{row[2]}\r\n\t{utf8}')
                                except Exception as E:
                                    pass
                                    #print("ERROR::CONV::HEX->ASCII::{opreturnText}::{E}")
                cursor.close()
                conn.commit()
        except Exception as E:
            print(f'ERROR::{E}')
        finally:
            pass

    ##
    #A worker to insert new blocks and build inital database
    def rpcBlockInsertWorker(self):
        #Create a new RPC connection to maximize performance threaded
        btcRpc = AuthServiceProxy("http://%s:%s@%s:%s"%(
            os.environ.get('BTCRPC_USER'),
            os.environ.get('BTCRPC_PASS'),
            os.environ.get('BTCRPC_HOST'),
            os.environ.get('BTCRPC_PORT')),
            timeout = 500)
        #Woker loop
        while True:
            try:
                if(self.getblockQ.empty()):
                    return 0
                else:
                    #Postgresql Connection
                    conn = self.pgPool.getconn()
                    #Fetch a list of block hashes for work to do
                    blockHashes = btcRpc.batch_(self.getblockQ.get_nowait())
                    #Now get all of the block data from the previous hashes
                    blocksQ = queue.Queue()
                    blocks = btcRpc.batch_([["getblock",h,2] for h in blockHashes])
                    for block in blocks:
                        print(Back.MAGENTA + Style.DIM + str(block['height']), end='')
                        print(Style.RESET_ALL)
                        for tx in block['tx']:
                            for vout in tx['vout']:
                                voutAsm = vout['scriptPubKey']['asm']
                                if 'OP_RETURN' in voutAsm:
                                    #print(voutAsm)
                                    #Blocksize Patch
                                    _queryText = f'INSERT INTO op_return_onchain (txid,vout,block,script_pubkey,value,row_hash) VALUES (%s, %s, %s, %s, %s, digest(CONCAT(CAST(%s AS text),%s,CAST(%s AS text),%s), \'sha256\')) ON CONFLICT (row_hash) DO NOTHING;'
                                    _queryVars = [tx['txid'], vout['n'], block['height'], vout['scriptPubKey']['asm'], vout['value'],
                                                  block['height'],tx['txid'],vout['n'],vout['scriptPubKey']['asm']]
                                    #If there is a valid connection that
                                    if(conn):
                                        cursor = conn.cursor()
                                        cursor.execute(_queryText,_queryVars)
                                        cursor.close()
                                        conn.commit()
                    self.pgPool.putconn(conn)
                    self.getblockQ.task_done()
            except Exception as E:
                print(E)

    def pgWriteBlocksToTip(self):
        chainHeight = self.btcRpc.getblock(self.btcRpc.getbestblockhash())['height']
        _maxBatch = 1
        _batchCmds = []
        for blockHeight in range(0,chainHeight):
            if((blockHeight % _maxBatch) == 0 and (len(_batchCmds) != 0)):
                self.getblockQ.put(_batchCmds)
                _batchCmds = []
            _batchCmds.append(["getblockhash",blockHeight])
        #Put any remaining _batch commands in the queue
        self.getblockQ.put(_batchCmds)
        #Now spawn some workers to do the heavy lifing
        threads = []
        try:
            for i in range(0,self.btcRpcWorkers):
                thread = threading.Thread(target=self.rpcBlockInsertWorker, daemon=True, args=())
                threads.append(thread)
            ### Fire up all of our threads
            for thread in threads:
                time.sleep(1)
                print(f"Spawning Bitcoin Core RPC worker...")
                thread.start()
            ### Wait until they are all done and join up
            for thread in threads:
                thread.join()
        except Exception as E:
            print("ERROR: Block processing failed with error" + str(E))
        finally:
            pass


dbLoader = OpReturnPGSQL(88)
