{-# LANGUAGE InstanceSigs, TupleSections #-}

module Kvstore.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy         as T
import qualified Data.HashSet           as Set
import qualified Data.HashMap.Strict    as Map
import           Control.Monad.State
import           Data.IORef
import qualified Data.Vector            as Vector
import           Data.Maybe
import           Data.Int

import           Kvstore.Cache
import           Kvstore.KVSTypes

import qualified DB_Iface               as DB

read_ :: T.Text -> T.Text -> Maybe (Set.HashSet T.Text) -> StateT (KVSState a b) IO KVResponse
read_ table key (Just fields) = do
  tables <- return . getKvs =<< get
  case (Map.lookup table tables) of
    Nothing -> return $ KVResponse READ Nothing Nothing $ Just $ T.pack "no such table!"
    (Just valTable) -> case (Map.lookup key valTable) of
                          Nothing -> return $ KVResponse READ Nothing Nothing $ Just $ T.pack "no such key!"
                          (Just fieldVals) -> return $ KVResponse
                                                         READ
                                                         (Just $ findFields fieldVals fields)
                                                         Nothing
                                                         Nothing
  where
    findFields fieldVals = (Map.fromList . catMaybes . (map (\k -> (k,) <$> Map.lookup k fieldVals)) . Set.toList)

read_ table key Nothing = return $ KVResponse READ (Just Map.empty) Nothing Nothing

scan :: T.Text -> T.Text -> Maybe Int32 -> StateT (KVSState a b) IO KVResponse
scan table key (Just recordCount) = do
  tables <- return . getKvs =<< get
  case (Map.lookup table tables) of
    Nothing -> return $ KVResponse SCAN Nothing Nothing $ Just $ T.pack "no such key!"
    (Just valTable) -> do
                        let collected = collect recordCount $ Map.toList valTable
                        return $ KVResponse SCAN Nothing (Just collected) Nothing
  where
    collect remaining [] = Vector.empty
    collect remaining ((k,v):xs) = if remaining >= recordCount
                                    then -- did not gather anything yet
                                      if k == key
                                        then Vector.singleton v Vector.++ collect (remaining - 1) xs
                                        else collect remaining xs
                                    else -- already gathering values
                                      if remaining > 0
                                        then Vector.singleton v Vector.++ collect (remaining - 1) xs
                                        else Vector.empty
scan table key Nothing = return $ KVResponse SCAN Nothing Nothing $ Just $ T.pack "no record count specified!"

update :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a b) IO KVResponse
update table key Nothing = update table key $ Just Map.empty
update table key (Just values) = do
   (KVSState tables db serde) <- get
   case (Map.lookup table tables) of
     Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) -> do
       case (Map.lookup key valTable) of
         Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such key!"
         (Just fields) -> do
           let fields' =  Map.union values fields
           let valTable' = Map.insert key fields' valTable
           let kvs' = Map.insert table valTable' tables
           put $ KVSState kvs' db serde
           return $ KVResponse UPDATE (Just Map.empty) Nothing Nothing

insert :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a b) IO KVResponse
insert table key Nothing = insert table key $ Just Map.empty
insert table key (Just values) = do
   (KVSState kvs db serde) <- get
   case (Map.lookup table kvs) of
     Nothing -> return $ KVResponse INSERT Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) -> do
                          let valTable' = Map.insert key values valTable
                          let kvs' = Map.insert table valTable' kvs
                          put $ KVSState kvs' db serde
                          return $ KVResponse INSERT (Just Map.empty) Nothing Nothing

delete :: T.Text -> T.Text -> StateT (KVSState a b) IO KVResponse
delete table key = do
  (KVSState kvs db serde) <- get
  case (Map.lookup table kvs) of -- probably something that should be done even before request processing
    Nothing -> return $ KVResponse DELETE Nothing Nothing $ Just $ T.pack "no such table!"
    (Just valTable) -> do
                         let valTable' = Map.delete key valTable
                         let kvs' = Map.adjust (\_ -> valTable') key kvs
                         put $ KVSState kvs' db serde
                         return $ KVResponse DELETE Nothing Nothing Nothing

handleReq :: KVRequest -> StateT (KVSState a b) IO KVResponse
handleReq (KVRequest op table key fields recordCount values) = do
  case op of
    READ   -> read_ table key fields
    SCAN   -> scan table key recordCount
    UPDATE -> update table key values
    INSERT -> insert table key values
    DELETE -> delete table key

execRequests :: (DB.DB_Iface a, SerDe b) => (Vector.Vector KVRequest) -> StateT (KVSState a b) IO (Vector.Vector KVResponse)
execRequests reqs = do
  -- cache management first
  refreshCache reqs
  -- request handling afterwards
  responses <- mapM handleReq =<< return reqs
  return responses

instance (DB.DB_Iface a, SerDe b) => KeyValueStore_Iface (KVSHandler a b) where
  requests :: KVSHandler a b -> (Vector.Vector KVRequest) -> IO (Vector.Vector KVResponse)
  requests (KVSHandler stateRef) reqs = do
    state <- readIORef stateRef
    (responses,state') <- runStateT (execRequests reqs) state
    _ <- writeIORef stateRef state'
    return responses
