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

type KVStore = Map.HashMap T.Text     -- table
                   (Map.HashMap T.Text  -- key
                        (Map.HashMap T.Text T.Text)) -- fields

data KVSHandler = KVSHandler (IORef KVStore)

serializeFields :: Map.HashMap T.Text T.Text -> T.Text
serializeFields fields = undefined -- FIXME there must exist something that does that in the Thrift code

read_ :: T.Text -> T.Text -> Maybe (Set.HashSet T.Text) -> StateT KVStore IO KVResponse
read_ table key (Just fields) = do
  kvs <- get
  case (Map.lookup table kvs) of
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

scan :: T.Text -> T.Text -> Maybe Int32 -> StateT KVStore IO KVResponse
scan table key (Just recordCount) = do
  kvs <- get
  case (Map.lookup table kvs) of
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

update :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT KVStore IO KVResponse
update table key Nothing = update table key $ Just Map.empty
update table key (Just values) = do
   kvs <- get
   case (Map.lookup table kvs) of
     Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) -> do
       case (Map.lookup key valTable) of
         Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such key!"
         (Just fields) -> do
           let fields' =  Map.union values fields
           let valTable' = Map.insert key fields' valTable
           let kvs' = Map.insert table valTable' kvs
           put kvs'
           return $ KVResponse UPDATE (Just Map.empty) Nothing Nothing

insert :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT KVStore IO KVResponse
insert table key Nothing = insert table key $ Just Map.empty
insert table key (Just values) = do
   kvs <- get
   case (Map.lookup table kvs) of
     Nothing -> return $ KVResponse INSERT Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) -> do
                          let valTable' = Map.insert key values valTable
                          let kvs' = Map.insert table valTable' kvs
                          put kvs'
                          return $ KVResponse INSERT (Just Map.empty) Nothing Nothing

delete :: T.Text -> T.Text -> StateT KVStore IO KVResponse
delete table key = do
  kvs <- get
  case (Map.lookup table kvs) of -- probably something that should be done even before request processing
    Nothing -> return $ KVResponse DELETE Nothing Nothing $ Just $ T.pack "no such table!"
    (Just valTable) -> do
                         let valTable' = Map.delete key valTable
                         put $ Map.adjust (\_ -> valTable') key kvs
                         return $ KVResponse DELETE Nothing Nothing Nothing

refreshCache :: T.Text -> StateT KVStore IO ()
refreshCache table = do
  kvs <- get
  if Map.member table kvs
    then return ()
    else return undefined -- TODO fetch table from a different node and load it into the kvs

handleReq :: KVRequest -> StateT KVStore IO KVResponse
handleReq (KVRequest op table key fields recordCount values) = do
  refreshCache table
  case op of
    READ -> read_ table key fields
    SCAN -> scan table key recordCount
    UPDATE -> update table key values
    INSERT -> insert table key values
    DELETE -> delete table key

execRequests :: (Vector.Vector KVRequest) -> StateT KVStore IO (Vector.Vector KVResponse)
execRequests reqs = do
  responses <- mapM handleReq =<< return reqs
  return responses

instance KeyValueStore_Iface KVSHandler where
  requests :: KVSHandler -> (Vector.Vector KVRequest) -> IO (Vector.Vector KVResponse)
  requests (KVSHandler cacheRef) reqs = do
    cache <- readIORef cacheRef
    (responses,cache') <- runStateT (execRequests reqs) cache
    _ <- writeIORef cacheRef cache'
    return responses
