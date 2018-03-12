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
import           Kvstore.Ohua.KVSTypes
import           Control.DeepSeq            as DS
import           Debug.Trace

import           Kvstore.Ohua.RequestHandling

--
-- algos
--

update :: (DB.DB_Iface db)
       => KVStore -> db -> T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text)
       -> OhuaM KVResponse
update cache db tableId key Nothing = update cache db tableId key $ Just HM.empty
update cache db tableId key (Just values) = do
  table' <- liftWithIndex calcUpdateStateIdx (calculateUpdate cache tableId key) values
  serializedTable <- liftWithIndex updateSerializeTableStateIdx serializeTable table'
  _ <- liftWithIndex updateStoreTableStateIdx (storeTable db tableId) serializedTable
  return $ KVResponse UPDATE (Just HM.empty) Nothing Nothing

insert :: (DB.DB_Iface db)
       => KVStore -> db -> T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text)
       -> OhuaM KVResponse
insert cache db tableId key Nothing = insert cache db tableId key $ Just HM.empty
insert cache db tableId key (Just values) = do
  table' <- liftWithIndex calcInsertStateIdx (calculateInsert cache tableId key) values
  serializedTable <- liftWithIndex insertSerializeTableStateIdx serializeTable table'
  _ <- liftWithIndex insertStoreTableStateIdx (storeTable db tableId) serializedTable
  return $ KVResponse INSERT (Just HM.empty) Nothing Nothing

delete :: (DB.DB_Iface db)
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


serve :: (DB.DB_Iface a)
      => KVStore -> a -> KVRequest -> OhuaM KVResponse
serve cache db (KVRequest op tableId key fields recordCount values) = do
  resp <- case_ op
    [
      (READ   , liftWithIndex rEADReqHandlingStateIdx (read_ cache tableId key) fields)
    , (SCAN   , liftWithIndex sCANReqHandlingStateIdx (scan cache tableId key) recordCount)
    , (UPDATE , update cache db tableId key values)
    , (INSERT , insert cache db tableId key values)
    , (DELETE , delete cache db tableId key)
    ]
  return resp
