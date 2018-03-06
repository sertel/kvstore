{-# LANGUAGE FlexibleContexts #-}

module Kvstore.Ohua.FBM.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy                       as T
import qualified Data.HashSet                         as Set
import qualified Data.HashMap.Strict                  as Map
import           Control.Monad.State
import           Data.IORef
import qualified Data.Vector                          as Vector
import           Data.Maybe
import           Data.Int

import qualified Kvstore.Cache                        as Cache
import           Kvstore.KVSTypes

import qualified DB_Iface                             as DB
import           Debug.Trace

import           FuturesBasedMonad
import           Control.DeepSeq

import qualified Kvstore.Ohua.FBM.Cache               as CacheO
import           Kvstore.Ohua.FBM.KVSTypes
import qualified Kvstore.Ohua.FBM.RequestHandling     as RH (serve)

foldEvictFromCache :: KVStore -> Vector.Vector KVRequest -> StateT Stateless IO KVStore
foldEvictFromCache cache = return . foldl (\c r -> Map.delete (kVRequest_table r) c) cache . Cache.findWrites

foldIntoCache :: KVStore -> [Maybe (T.Text, Table)] -> StateT Stateless IO KVStore
foldIntoCache =
  foldM (\c e ->
            case e of
              (Just entry) -> do
                (_, KVSState c' _ _ _) <- liftIO $ runStateT (Cache.insertTableIntoCache entry) $ KVSState c undefined undefined undefined
                return c'
              Nothing -> return c)

foldINSERTsIntoCache :: KVStore -> [KVRequest] -> StateT Stateless IO KVStore
foldINSERTsIntoCache =
  foldM (\c req -> do
                (_, KVSState c' _ _ _) <- liftIO $ runStateT (Cache.mergeINSERTIntoCache req) $ KVSState c undefined undefined undefined
                return c')

execRequestsOhua :: (DB.DB_Iface db)
                 => KVStore -> db -> Vector.Vector KVRequest
                 -> OhuaM (Vector.Vector KVResponse, KVStore, db)
execRequestsOhua cache db reqs = do

  -- FIXME if the db is folded over then this also turns into a fold.
  --       this fold can later on be optimized in the streams version
  --       because only the final step of loading the data is essentially
  --       to be folded over!
  newEntries <- smap (CacheO.loadCacheEntry cache db) [kVRequest_table req | req <- Vector.toList reqs]

  cache' <- liftWithIndex foldIntoCacheStateIdx (foldIntoCache cache) newEntries

  cache'' <- liftWithIndex foldINSERTsIntoCacheStateIdx (foldINSERTsIntoCache cache') $ Vector.toList $ Cache.findInserts reqs

  responses <- smap (RH.serve cache'' db) $ Vector.toList reqs
  cache''' <- liftWithIndex foldEvictFromCacheStateIdx (foldEvictFromCache cache'') reqs
  return (Vector.fromList responses, cache''', db)


execRequestsFunctional :: (DB.DB_Iface db, NFData db)
                       => Vector.Vector KVRequest
                       -> StateT (KVSState db) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs = do
  (KVSState cache db ser deser) <- get
  ((responses, cache'', db''), serde')
                <- liftIO $ runOhuaM (execRequestsOhua cache db reqs) $ globalState ser deser
  let (ser',deser') = convertState serde'
  put $ KVSState cache'' db'' ser' deser'
  return responses
