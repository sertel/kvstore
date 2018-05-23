{-# LANGUAGE FlexibleContexts #-}

module Kvstore.Ohua.FBM.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import           Control.Monad.State
import           Control.Lens
import qualified Data.Text.Lazy                       as T
import qualified Data.HashSet                         as Set
import qualified Data.HashMap.Strict                  as Map
import           Data.IORef
import qualified Data.Vector                          as Vector
import           Data.Maybe
import           Data.Int

import qualified Kvstore.Cache                        as Cache
import           Kvstore.KVSTypes

import qualified DB_Iface                             as DB
import           Debug.Trace

import           Monad.FuturesBasedMonad
import           Control.DeepSeq

import           Kvstore.Ohua.KeyValueService
import qualified Kvstore.Ohua.FBM.Cache               as CF
import           Kvstore.Ohua.KVSTypes
import qualified Kvstore.Ohua.FBM.RequestHandling     as RH (serve)

execRequestsOhua :: (DB.DB_Iface db)
                 => KVStore -> db -> Vector.Vector KVRequest
                 -> OhuaM (Vector.Vector KVResponse, KVStore, db)
execRequestsOhua cache db reqs = do

  -- FIXME if the db is folded over then this also turns into a fold.
  --       this fold can later on be optimized in the streams version
  --       because only the final step of loading the data is essentially
  --       to be folded over!

  newEntries <- smap (CF.loadCacheEntry cache db) [kVRequest_table req | req <- Vector.toList reqs]

  cache' <- liftWithIndex foldIntoCacheStateIdx (foldIntoCache cache) newEntries

-- make sure the tests still run after removing this
--  cache'' <- liftWithIndex foldINSERTsIntoCacheStateIdx (foldINSERTsIntoCache cache') $ Vector.toList $ Cache.findInserts reqs


  let cache'' = cache'

  responses <- smap (RH.serve cache'' db) $ Vector.toList reqs

  cache''' <- liftWithIndex foldEvictFromCacheStateIdx (foldEvictFromCache cache'') reqs

  return (Vector.fromList responses, cache''', db)


execRequestsFunctional :: (DB.DB_Iface db, NFData db)
                       => Vector.Vector KVRequest
                       -> StateT (KVSState db) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs = do
  kvsstate@KVSState{ _cache=cache_, _storage=db
                   , _serializer=ser, _deserializer=deser
                   , _compression=comp, _decompression=decomp
                   , _encryption=enc, _decryption=dec } <- get
  ((responses, cache'', db''), serde')
                <- liftIO $ runOhuaM (execRequestsOhua cache_ db reqs) $ globalState ser deser comp decomp enc dec
  let (ser',deser',comp',decomp',enc',dec') = convertState serde'
  put kvsstate{ _storage=db'', _cache=cache''
              , _serializer=ser', _deserializer=deser'
              , _compression=comp', _decompression=decomp'
              , _encryption=enc', _decryption=dec' }
  return responses
