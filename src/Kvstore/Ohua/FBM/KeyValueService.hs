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
import qualified Kvstore.Ohua.FBM.RequestHandling     as RH

foldEvictFromCache :: KVStore -> Vector.Vector KVRequest -> StateT (LocalState serde) IO KVStore
foldEvictFromCache cache = return . foldl (\c r -> Map.delete (kVRequest_table r) c) cache . Cache.findWrites

foldIntoCache :: SerDe serde => KVStore -> [Maybe (T.Text, Table)] -> StateT (LocalState serde) IO KVStore
foldIntoCache =
  foldM (\c e ->
            case e of
              (Just entry) -> do
                (_, KVSState c' _ _) <- liftIO $ runStateT (Cache.updateCacheEntry entry) $ KVSState c undefined undefined
                return c'
              Nothing -> return c)

execRequestsOhua :: (DB.DB_Iface db, SerDe serde,
                     -- PC.NFData KVResponse,
                     NFData (LocalState serde))
                 => KVStore -> db -> Vector.Vector KVRequest
                 -> OhuaM (LocalState serde) (Vector.Vector KVResponse, KVStore, db)
execRequestsOhua cache db reqs = do

  -- FIXME if the db is folded over then this also turns into a fold.
  --       this fold can later on be optimized in the streams version
  --       because only the final step of loading the data is essentially
  --       to be folded over!
  newEntries <- smap (CacheO.loadCacheEntry cache db) [kVRequest_table req | req <- Vector.toList reqs]

  cache' <- liftWithIndex (-1) (foldIntoCache cache) newEntries

  responses <- smap (RH.serve cache' db) $ Vector.toList reqs

  cache'' <- liftWithIndex (-1) (foldEvictFromCache cache) reqs
  return (Vector.fromList responses, cache'', db)

execRequestsFunctional :: (DB.DB_Iface db,
                           SerDe serde,
                           NFData db,
                           NFData (LocalState serde))
                       => Vector.Vector KVRequest
                       -> StateT (KVSState db serde) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs = do
  (KVSState cache db serde) <- get
  ((responses, cache'', db''), Deserializer serde'':_)
                <- liftIO $ runOhuaM (execRequestsOhua cache db reqs)
                                     [Deserializer serde, Serializer serde] -- TODO define the global state!
  put $ KVSState cache'' db'' serde''
  return responses
