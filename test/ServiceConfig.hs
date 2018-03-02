{-# LANGUAGE InstanceSigs, FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}

module ServiceConfig where

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import           Data.IORef

import qualified DB_Iface                  as DB
import           Db_Types

type MockDB = IORef (HM.HashMap T.Text T.Text)

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO DBResponse
  get dbRef key = DBResponse . HM.lookup key <$> readIORef dbRef

  put :: MockDB -> T.Text -> T.Text -> IO ()
  put dbRef key value = do
    db <- readIORef dbRef
    let convert = \case (Just p) -> p; Nothing -> T.empty
    let db' = HM.insert key value db
    writeIORef dbRef db'
