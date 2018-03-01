{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Kvstore.Ohua.FBM.RequestHandling where

import           Data.Int
import qualified Data.Text.Lazy             as T
import qualified Data.HashSet               as Set
import qualified Data.HashMap.Strict     as HM
import qualified Data.ByteString.Lazy    as BS
import           Data.Maybe
import           Control.Monad.State
import           Control.Monad.IO.Class

import           KeyValueStore_Iface
import           Kvservice_Types

import qualified DB_Iface                   as DB
import           Kvstore.KVSTypes
import qualified Kvstore.RequestHandling    as RH
import qualified Kvstore.InputOutput        as InOut

import           FuturesBasedMonad
import           Kvstore.Ohua.FBM.KVSTypes
import           Control.DeepSeq            as DS

read_ :: SerDe serde => KVStore -> T.Text -> T.Text -> Maybe (Set.HashSet T.Text) -> StateT (LocalState serde) IO KVResponse
read_ cache tableId key fields = liftIO . flip evalStateT (KVSState cache undefined undefined) $ RH.read_ tableId key fields
-- FIXME Why is this not working?!
-- read_ cache = liftIO . flip evalStateT (KVSState cache undefined undefined) . RH.read_

scan :: SerDe serde => KVStore -> T.Text -> T.Text -> Maybe Int32 -> StateT (LocalState serde) IO KVResponse
scan cache tableId key count = liftIO . flip evalStateT (KVSState cache undefined undefined) $ RH.scan tableId key count

calculateUpdate :: SerDe serde => KVStore -> T.Text -> T.Text -> HM.HashMap T.Text T.Text -> StateT (LocalState serde) IO Table
calculateUpdate cache tableId key values =
  let table = case HM.lookup tableId cache of { (Just t) -> t; Nothing -> HM.empty }
      vals' = case HM.lookup key table of
            Nothing -> values
            (Just vals) -> HM.union values vals
      table' = HM.insert key vals' table
  in return table'

calculateInsert :: SerDe serde => KVStore -> T.Text -> T.Text -> HM.HashMap T.Text T.Text -> StateT (LocalState serde) IO Table
calculateInsert cache tableId key values = return $ case HM.lookup tableId cache of
                                                      (Just table) -> HM.insert key values table
                                                      Nothing -> HM.singleton key values

calculateDelete :: SerDe serde => KVStore -> T.Text -> T.Text -> HM.HashMap T.Text T.Text -> StateT (LocalState serde) IO Table
calculateDelete cache tableId key values = return $ case HM.lookup tableId cache of
                                                      (Just table) -> HM.insert key values table
                                                      Nothing -> HM.singleton key values

serializeTable :: SerDe serde => Table -> StateT (LocalState serde) IO BS.ByteString
serializeTable table = do
  (Serializer serde) <- get
  (r, KVSState _ _ serde') <- liftIO $ runStateT (InOut.serializeTable table) (KVSState undefined undefined serde)
  put $ Serializer serde'
  return r

storeTable :: (DB.DB_Iface db, SerDe serde) => db -> T.Text -> BS.ByteString -> StateT (LocalState serde) IO ()
storeTable db tableId value = liftIO $ evalStateT (InOut.storeTable tableId value) $ KVSState undefined db undefined

--
-- algos
--

update :: (DB.DB_Iface db, SerDe serde)
       => KVStore -> db -> T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text)
       -> OhuaM KVResponse
update cache db tableId key Nothing = update cache db tableId key $ Just HM.empty
update cache db tableId key (Just values) = do
  table' <- liftWithIndex calcUpdateStateIdx (calculateUpdate cache tableId key) values
  serializedTable <- liftWithIndex updateSerializeTableStateIdx serializeTable table'
  _ <- liftWithIndex updateStoreTableStateIdx (storeTable db tableId) serializedTable
  return $ KVResponse UPDATE (Just HM.empty) Nothing Nothing

insert :: (DB.DB_Iface db, SerDe serde)
       => KVStore -> db -> T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text)
       -> OhuaM KVResponse
insert cache db tableId key Nothing = insert cache db tableId key $ Just HM.empty
insert cache db tableId key (Just values) = do
  table' <- liftWithIndex calcInsertStateIdx (calculateInsert cache tableId key) values
  serializedTable <- liftWithIndex insertSerializeTableStateIdx serializeTable table'
  _ <- liftWithIndex insertStoreTableStateIdx (storeTable db tableId) serializedTable
  return $ KVResponse INSERT (Just HM.empty) Nothing Nothing

delete :: (DB.DB_Iface db, SerDe serde)
       => KVStore -> db -> T.Text -> T.Text
       -> OhuaM KVResponse
delete cache db tableId key = do
  let table = HM.lookup tableId cache
  case_ (isJust table)
        [
          (True, do
              serializedTable <- liftWithIndex deleteSerializeTableStateIdx serializeTable $ HM.delete key $ fromJust table
              liftWithIndex deleteStoreTableStateIdx (storeTable db tableId) serializedTable)
        , (False, return ())
        ]
  return $ KVResponse DELETE (Just HM.empty) Nothing Nothing


serve :: (DB.DB_Iface a, SerDe serde)
      => KVStore -> a -> KVRequest -> OhuaM KVResponse
serve cache db (KVRequest op tableId key fields recordCount values) =
  case_ op
    [
      (READ   , liftWithIndex rEADReqHandlingStateIdx (read_ cache tableId key) fields)
    , (SCAN   , liftWithIndex sCANReqHandlingStateIdx (scan cache tableId key) recordCount)
    , (UPDATE , update cache db tableId key values)
    , (INSERT , insert cache db tableId key values)
    , (DELETE , delete cache db tableId key)
    ]
