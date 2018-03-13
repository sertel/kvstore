{-# LANGUAGE FlexibleContexts #-}

module Kvstore.Ohua.SBFM.KeyValueService where

import           Control.Monad.State
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

import           Kvstore.Ohua.KeyValueService
import           Kvstore.Ohua.KVSTypes
import qualified Kvstore.Ohua.SBFM.Cache           as CF
import           Kvstore.Ohua.SBFM.KVSTypes        as SFBMTypes
import qualified Kvstore.Ohua.SBFM.RequestHandling as RH (serve)


execRequestsOhua :: (DB.DB_Iface db, Typeable db)
                 => Var KVStore -> Var db -> Var (Vector.Vector KVRequest)
                 -> ASTM [Dynamic] (Var (Vector.Vector KVResponse, KVStore, db))
execRequestsOhua cache db reqs = do

  -- FIXME if the db is folded over then this also turns into a fold.
  --       this fold can later on be optimized in the streams version
  --       because only the final step of loading the data is essentially
  --       to be folded over!
  genReq <- liftWithIndex reqGeneratorStateIdx
                          ((\r -> return [kVRequest_table req | req <- Vector.toList r]) :: Vector.Vector KVRequest ->  StateT Stateless IO [T.Text])
                          reqs
  newEntries <- smap (CF.loadCacheEntry cache db) genReq

  cache' <- lift2WithIndex foldIntoCacheStateIdx
                           foldIntoCache cache newEntries

  cache'' <- lift2WithIndex foldINSERTsIntoCacheStateIdx
                           (\c r -> foldINSERTsIntoCache c $ Vector.toList $ Cache.findInserts r)
                           cache' reqs

  listReq <- liftWithIndex reqsToListStateIdx
                           (return . Vector.toList :: Vector.Vector KVRequest -> StateT Stateless IO [KVRequest])
                           reqs
  responses <- smap (RH.serve cache'' db) listReq
  cache''' <- lift2WithIndex foldEvictFromCacheStateIdx foldEvictFromCache cache'' reqs
  lift3WithIndex finalResultStateIdx
                 ((\r c d -> return (Vector.fromList r, c, d)) :: [KVResponse] -> KVStore -> db -> StateT Stateless IO (Vector.Vector KVResponse, KVStore, db))
                 responses cache''' db


execRequestsFunctional :: (DB.DB_Iface db, Typeable db)
                       => Vector.Vector KVRequest
                       -> StateT (KVSState db) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs = do
  (KVSState cache db ser deser) <- get
  (responses, cache'', db'')
                <- liftIO $ runOhuaM -- (execRequestsOhua =<< (sfConst' cache) =<< (sfConst' db) =<< (sfConst' reqs))
                                     (do
                                       c <- sfConst' cache
                                       d <- sfConst' db
                                       r <- sfConst' reqs
                                       execRequestsOhua c d r)
                                     $ SFBMTypes.globalState ser deser
  -- let (ser',deser') = convertState serde'
  put $ KVSState cache'' db'' ser deser
  return responses
