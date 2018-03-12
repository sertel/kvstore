
module Kvstore.Ohua.Cache where

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

import           Kvstore.Ohua.KVSTypes

loadTableSF :: (DB.DB_Iface a) => a -> T.Text -> StateT Stateless IO (Maybe BS.ByteString)
loadTableSF db tableId = liftIO $ evalStateT (loadTable tableId) $ KVSState undefined db undefined undefined

deserializeTableSF :: BS.ByteString -> StateT Deserialization IO Table
deserializeTableSF d = do
  deser <- get
  (r, KVSState _ _ _ deser') <- liftIO $ runStateT (deserializeTable d) $ KVSState undefined undefined undefined deser
  put deser'
  return r
