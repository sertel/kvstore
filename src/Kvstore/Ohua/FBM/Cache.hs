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

import           Monad.FuturesBasedMonad
import           Control.DeepSeq
import           Kvstore.Ohua.KVSTypes
import           Kvstore.Ohua.Cache

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
