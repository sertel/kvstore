{-# LANGUAGE InstanceSigs #-}

module Kvstore.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy          as T
import qualified Data.HashSet            as Set
import qualified Data.HashMap.Strict     as Map
import           Control.Monad.State
import           Data.IORef
import qualified Data.Vector             as Vector
import           Data.Maybe
import           Data.Int

import qualified Kvstore.Cache           as Cache
import qualified Kvstore.RequestHandling as RH
import           Kvstore.KVSTypes

import qualified DB_Iface                as DB


execRequests :: (DB.DB_Iface a, SerDe b) => (Vector.Vector KVRequest) -> StateT (KVSState a b) IO (Vector.Vector KVResponse)
execRequests reqs = do
  -- cache management first
  Cache.refresh reqs
  -- request handling afterwards
  responses <- mapM RH.serve =<< return reqs
  return responses

instance (DB.DB_Iface a, SerDe b) => KeyValueStore_Iface (KVSHandler a b) where
  requests :: KVSHandler a b -> (Vector.Vector KVRequest) -> IO (Vector.Vector KVResponse)
  requests (KVSHandler stateRef) reqs = do
    state <- readIORef stateRef
    (responses,state') <- runStateT (execRequests reqs) state
    _ <- writeIORef stateRef state'
    return responses
