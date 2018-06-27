{-# LANGUAGE FlexibleContexts, OverloadedStrings #-}

module Kvstore.Ohua.SBFM.KeyValueService where

import           Control.Monad.State
import           Control.Lens
import qualified Data.HashMap.Strict               as Map
import qualified Data.HashSet                      as Set
import           Data.Int
import           Data.IORef
import           Data.Maybe
import qualified Data.Text.Lazy                    as T
import qualified Data.Vector                       as Vector
import           KeyValueStore_Iface
import           Kvservice_Types

import qualified Kvstore.Cache                     as Cache
import           Kvstore.KVSTypes

import qualified DB_Iface                          as DB
import           Debug.Trace

import           Data.Dynamic2
import           Monad.StreamsBasedExplicitAPI
import           Monad.StreamsBasedFreeMonad
import Control.Monad.Stream.Chan

import Kvstore.KeyValueService
import           Kvstore.Ohua.KeyValueService
import           Kvstore.Ohua.KVSTypes
import qualified Kvstore.Ohua.SBFM.Cache           as CF
import           Kvstore.Ohua.SBFM.KVSTypes        as SFBMTypes
import qualified Kvstore.Ohua.SBFM.RequestHandling as RH

import Control.Arrow (first)
import qualified Data.Map as M
import System.CPUTime


execRequestsOhua ::
       (DB.DB_Iface db, Typeable db)
    => Var KVStore
    -> Var db
    -> Var (Vector.Vector KVRequest)
    -> ASTM [Dynamic] (Var (Vector.Vector KVResponse, KVStore, db))
execRequestsOhua cache db reqs
  -- FIXME if the db is folded over then this also turns into a fold.
  --       this fold can later on be optimized in the streams version
  --       because only the final step of loading the data is essentially
  --       to be folded over!
 = do
    genReq <-
        liftWithIndexNamed
            reqGeneratorStateIdx
            "kvstore/req-tables"
            ((\r -> pure [kVRequest_table req | req <- Vector.toList r]) :: Vector.Vector KVRequest -> StateT Stateless IO [T.Text])
            reqs
    newEntries <- smap (CF.loadCacheEntry cache db) genReq
    cache' <-
        lift2WithIndexNamed
            foldIntoCacheStateIdx
            "kvstore/fold-into-cache"
            foldIntoCache
            cache
            newEntries
  -- cache'' <- lift2WithIndex foldINSERTsIntoCacheStateIdx
  --                          (\c r -> foldINSERTsIntoCache c $ Vector.toList $ Cache.findInserts r)
  --                          cache' reqs
    listReq <-
        liftWithIndexNamed
            reqsToListStateIdx
            "kvstore/reqs-to-list"
            (return . Vector.toList :: Vector.Vector KVRequest -> StateT Stateless IO [KVRequest])
            reqs
    db' <-
        do writeList <-
               liftWithIndexNamed
                   getWriteListIdx
                   "kvstore/filter-writes"
                   (RH.pureUnitSf . Vector.toList . Cache.findWrites)
                   reqs
           noWritesPresent <-
               liftWithIndexNamed
                   areWritesPresentIndex
                   "kvstore/are-writes-present"
                   (RH.pureUnitSf . null)
                   writeList
           if_ noWritesPresent (return db) $ do
               foldRes <-
                   lift2WithIndexNamed
                       foldWritesIntoCacheIdx
                       "kvstore/fold-writes-into-cache"
                       (\a b -> RH.pureUnitSf $ foldWritesIntoCache a b)
                       cache'
                       writeList
               touched <-
                   liftWithIndexNamed getTouchedIdx "kvstore/get-touched" (RH.pureUnitSf . fst) foldRes
               enrichedCache <-
                   liftWithIndexNamed
                       getEnrichedStateIdx
                       "kvstore/get-enriched-cache"
                       (RH.pureUnitSf . snd)
                       foldRes
               u <- RH.writeback enrichedCache db touched
               lift2WithIndexNamed seqDBIndex "kvstore/seq-db" (\db _ -> RH.pureUnitSfLazy db) db u
    responses <- smap (RH.serve cache' db) listReq
    cache''' <-
        lift2WithIndexNamed foldEvictFromCacheStateIdx "kvstore/evict" foldEvictFromCache cache' reqs
    lift3WithIndexNamed
        finalResultStateIdx
        "kvstore/compose-result"
        ((\r c d -> return (Vector.fromList r, c, d)) :: [KVResponse] -> KVStore -> db -> StateT Stateless IO ( Vector.Vector KVResponse
                                                                                                              , KVStore
                                                                                                              , db))
        responses
        cache'''
        db'


execRequestsFunctional ::
       (DB.DB_Iface db, Typeable db)
    => Vector.Vector KVRequest
    -> StateT (KVSState db) IO (Vector.Vector KVResponse)
execRequestsFunctional = fmap fst . execRequestsFunctional0


execRequestsFunctional0 reqs = do
    kvsstate@KVSState { _cache = cache_
                      , _storage = db
                      , _serializer = ser
                      , _deserializer = deser
                      , _compression = comp
                      , _decompression = decomp
                      , _encryption = enc
                      , _decryption = dec
                      } <- get
    ((responses, cache'', db''), stats) <-
        liftIO $
        runChanM $
        flip runAlgoWStats (SFBMTypes.globalState ser deser comp decomp enc dec) =<<
        liftIO
            (createAlgo
        -- (execRequestsOhua =<< (sfConst' cache) =<< (sfConst' db) =<< (sfConst' reqs))
                 (do c <- sfConst' cache_
                     d <- sfConst' db
                     r <- sfConst' reqs
                     execRequestsOhua c d r))
  -- let (ser',deser') = convertState serde' -- TODO
    put kvsstate {_storage = db'', _cache = cache''}
    return (responses, stats)
