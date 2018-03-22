{-# LANGUAGE InstanceSigs #-}

module Database.DatabaseService where

import qualified Data.Text.Lazy         as T
import           DB_Iface
import           Db_Types
import           Data.ByteString.Lazy   as BS

data UpdateOnLoad = UpdateOnLoad Int

-- this handler favors reads and appends writes.
-- it updates its via only on reads of the according key value
instance DB_Iface UpdateOnLoad where
  get :: UpdateOnLoad -> T.Text -> IO DBResponse
  get handler key = undefined

  put :: UpdateOnLoad -> T.Text -> BS.ByteString -> IO ()
  put handler key value = undefined


data UpdateOnStore = UpdateOnStore Int

-- on every write, the handler loads the according key-value pair into memory,
-- updates it and writes it back to disk.
instance DB_Iface UpdateOnStore where
  get :: UpdateOnStore -> T.Text -> IO DBResponse
  get handler key = undefined

  put :: UpdateOnStore -> T.Text -> BS.ByteString -> IO ()
  put handler key value = undefined
