{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Kvstore.Ohua.RequestHandling where

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

import           Kvstore.Ohua.KVSTypes
import           Debug.Trace


read_ ::
       KVStore
    -> T.Text
    -> T.Text
    -> Maybe (Set.HashSet T.Text)
    -> StateT Stateless IO KVResponse
read_ c tableId key fields =
    liftIO . flip evalStateT KVSState {_cache = c} $ RH.read_ tableId key fields

scan ::
       KVStore
    -> T.Text
    -> T.Text
    -> Maybe Int32
    -> StateT Stateless IO KVResponse
scan c tableId key count =
    liftIO . flip evalStateT KVSState {_cache = c} $ RH.scan tableId key count

calculateUpdate ::
       KVStore
    -> T.Text
    -> T.Text
    -> HM.HashMap T.Text T.Text
    -> StateT Stateless IO Table
calculateUpdate cache tableId key values =
    let table =
            case HM.lookup tableId cache of
                (Just t) -> t
                Nothing -> HM.empty
        vals' =
            case HM.lookup key table of
                Nothing -> values
                (Just vals) -> HM.union values vals
        table' = HM.insert key vals' table
     in return table'

calculateInsert ::
       KVStore
    -> T.Text
    -> T.Text
    -> HM.HashMap T.Text T.Text
    -> StateT Stateless IO Table
calculateInsert cache tableId key values =
    return $
    case HM.lookup tableId cache of
        (Just table) -> HM.insert key values table
        Nothing -> HM.singleton key values

calculateDelete ::
       KVStore
    -> T.Text
    -> T.Text
    -> HM.HashMap T.Text T.Text
    -> StateT Stateless IO Table
calculateDelete cache tableId key values =
    return $
    case HM.lookup tableId cache of
        (Just table) -> HM.insert key values table
        Nothing -> HM.singleton key values

-- serializeTable :: Table -> StateT Serialization IO BS.ByteString
-- serializeTable table = do
--     ser <- get
--     (r, KVSState {_serializer = ser'}) <-
--         liftIO $
--         runStateT (InOut.serializeTable table) KVSState {_serializer = ser}
--     put ser'
--     return r

-- compressTable :: BS.ByteString -> StateT Compression IO BS.ByteString
-- compressTable table = do
--     comp <- get
--     (r, KVSState {_compression = comp'}) <-
--         liftIO $
--         runStateT (InOut.compressTable table) KVSState {_compression = comp}
--     put comp'
--     return r

-- encryptTable :: BS.ByteString -> StateT Encryption IO BS.ByteString
-- encryptTable table = do
--     enc <- get
--     (r, KVSState {_encryption = enc'}) <-
--         liftIO $
--         runStateT (InOut.encryptTable table) KVSState {_encryption = enc}
--     put enc'
--     return r

storeTable ::
       (DB.DB_Iface db)
    => db
    -> T.Text
    -> BS.ByteString
    -> StateT Stateless IO ()
storeTable db tableId value =
    liftIO $
    evalStateT (InOut.storeTable tableId value) KVSState {_storage = db}
