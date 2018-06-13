{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE LambdaCase #-}

module Kvstore.Ohua.FBM.RequestHandling where

import           Data.Int
import qualified Data.Text.Lazy             as T
import qualified Data.HashSet               as Set
import qualified Data.HashMap.Strict     as HM
import qualified Data.ByteString.Lazy    as BS
import           Data.Maybe
import           Control.Monad.State
import           Control.Monad.IO.Class
import           Control.Lens

import           KeyValueStore_Iface
import           Kvservice_Types

import qualified DB_Iface                   as DB
import           Kvstore.KVSTypes
import qualified Kvstore.RequestHandling    as RH
import qualified Kvstore.InputOutput        as InOut

import           Monad.FuturesBasedMonad
import           Kvstore.Ohua.KVSTypes
import           Control.DeepSeq            as DS
import           Debug.Trace

import           Kvstore.Ohua.RequestHandling

--
-- algos
--

store :: (DB.DB_Iface db)
      => Int -> Int -> Int -> Int -> db -> T.Text -> Table -> OhuaM ()
store serializeTableStateIdx
      compressTableStateIdx
      encryptTableStateIdx
      storeTableStateIdx
      db tableId table = do
        serializedTable <- liftWithIndex serializeTableStateIdx serializeTable table
        compressedTable <- liftWithIndex compressTableStateIdx compressTable serializedTable
        encryptedTable <- liftWithIndex encryptTableStateIdx encryptTable compressedTable
        _ <- liftWithIndex storeTableStateIdx (storeTable db tableId) encryptedTable
        return ()

serveReads :: (DB.DB_Iface a) => KVStore -> a -> KVRequest -> OhuaM KVResponse
serveReads cache db (KVRequest op tableId key fields recordCount values) = do
  resp <- case_ op
    [
      (READ   , liftWithIndex rEADReqHandlingStateIdx (read_ cache tableId key) fields)
    , (SCAN   , liftWithIndex sCANReqHandlingStateIdx (scan cache tableId key) recordCount)
    , (UPDATE , stdResp UPDATE)
    , (INSERT , stdResp INSERT)
    , (DELETE , stdResp DELETE)
    ]

  return resp
  where stdResp ty = return $ KVResponse ty (Just mempty) Nothing Nothing


writeback :: DB.DB_Iface db => KVStore -> db -> Set.HashSet T.Text -> OhuaM ()
writeback cache db = void . smap (\t -> store writebackSerializeTableStateIdx
                        writebackCompressTableStateIdx
                        writebackEncryptTableStateIdx
                        writebackStoreTableStateIdx
                        db
                        t (cache HM.! t)) . Set.toList
