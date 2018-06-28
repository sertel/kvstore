{-# LANGUAGE FlexibleContexts #-}

module Kvstore.Ohua.FBM.KeyValueService where


import Control.DeepSeq
import Control.Lens
import Control.Monad.State
import qualified Data.HashMap.Strict as Map
import qualified Data.HashSet as Set
import Data.IORef
import Data.Int
import Data.Maybe
import qualified Data.Text.Lazy as T
import qualified Data.Vector as Vector

import Debug.Trace

import Monad.FuturesBasedMonad

import qualified DB_Iface as DB
import KeyValueStore_Iface
import Kvservice_Types
import qualified Kvstore.Cache as Cache
import Kvstore.KVSTypes
import Kvstore.KeyValueService
import Kvstore.Ohua.FBM.Cache as CF
import qualified Kvstore.Ohua.FBM.RequestHandling as RH
import Kvstore.Ohua.KVSTypes
import Kvstore.Ohua.KeyValueService


pureUnitSf :: a -> StateT () IO a
pureUnitSf = pure


execRequestsOhua ::
       (DB.DB_Iface db)
    => KVStore
    -> db
    -> Vector.Vector KVRequest
    -> OhuaM (Vector.Vector KVResponse, KVStore, db)
execRequestsOhua cache db reqs
  -- FIXME if the db is folded over then this also turns into a fold.
  --       this fold can later on be optimized in the streams version
  --       because only the final step of loading the data is essentially
  --       to be folded over!
 = do
    newEntries <-
        smap
            (CF.loadCacheEntry cache db)
            (Set.toList $ Set.fromList $ map kVRequest_table $ Vector.toList reqs)
    cache' <-
        liftWithIndex foldIntoCacheStateIdx (foldIntoCache cache) newEntries
  -- make sure the tests still run after removing this
  --  cache'' <- liftWithIndex foldINSERTsIntoCacheStateIdx (foldINSERTsIntoCache cache') $ Vector.toList $ Cache.findInserts reqs
    (responses, cache'', ()) <-
        (,,) <$> smap (RH.serveReads cache' db) (Vector.toList reqs) <*>
        liftWithIndex
            foldEvictFromCacheStateIdx
            (foldEvictFromCache cache')
            reqs <*>
        (do (touched, enrichedCache) <-
                liftWithIndex
                    foldWritesIntoCacheIdx
                    (pureUnitSf . foldWritesIntoCache cache') $
                Vector.toList $ Cache.findWrites reqs
            RH.writeback enrichedCache db touched)
    return (Vector.fromList responses, cache'', db)


execRequestsFunctional ::
       (DB.DB_Iface db, NFData db)
    => Vector.Vector KVRequest
    -> StateT (KVSState db) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs = do
    kvsstate@KVSState { _cache = cache_
                      , _storage = db
                      , _serializer = ser
                      , _deserializer = deser
                      , _compression = comp
                      , _decompression = decomp
                      , _encryption = enc
                      , _decryption = dec
                      } <- get
    ((responses, cache'', db''), serde') <-
        liftIO $
        runOhuaM (execRequestsOhua cache_ db reqs) $
        globalState ser deser comp decomp enc dec
    let (ser', deser', comp', decomp', enc', dec') = convertState serde'
    put kvsstate
            { _storage = db''
            , _cache = cache''
            , _serializer = ser'
            , _deserializer = deser'
            , _compression = comp'
            , _decompression = decomp'
            , _encryption = enc'
            , _decryption = dec'
            }
    return responses
