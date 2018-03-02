{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Kvstore.Ohua.FBM.Cache where

import           Kvservice_Types

import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import           Control.Monad.State
import qualified Data.Vector             as Vector
import qualified Data.HashMap.Strict     as Map
import qualified Data.Set                as Set
import           Data.Maybe

import qualified DB_Iface                as DB
import           Kvstore.KVSTypes
import           Kvstore.InputOutput

import           Debug.Trace

import           FuturesBasedMonad
import           Control.DeepSeq
import           Kvstore.Ohua.FBM.KVSTypes

loadTableSF :: (DB.DB_Iface a) => a -> T.Text -> StateT Stateless IO (Maybe BS.ByteString)
loadTableSF db tableId = liftIO $ evalStateT (loadTable tableId) $ KVSState undefined db undefined undefined

deserializeTableSF :: BS.ByteString -> StateT Deserialization IO Table
deserializeTableSF d = do
  deser <- get
  (r, KVSState _ _ _ deser') <- liftIO $ runStateT (deserializeTable d) $ KVSState undefined undefined undefined deser
  put deser'
  return r

-- algo
loadCacheEntry :: (DB.DB_Iface db)
                  => KVStore -> db -> T.Text -> OhuaM (Maybe (T.Text, Table))
loadCacheEntry kvs db tableId =
  let table = Map.lookup tableId kvs
  in
    case_ (isJust table)
      [
        (True , return $ Just (tableId, fromJust table))
      , (False , do
            serializedValTable <- liftWithIndex loadTableStateIdx (loadTableSF db) tableId
            case_ (isJust serializedValTable)
             [
               (True , Just . (tableId,) <$> liftWithIndex deserializeTableStateIdx deserializeTableSF (fromJust serializedValTable))
             , (False , return Nothing)
             ])
      ]
