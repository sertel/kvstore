{-# LANGUAGE InstanceSigs, FlexibleInstances #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import qualified Data.Text.Lazy         as T
import qualified Data.HashMap.Strict    as HM
import qualified Data.Vector            as V
import           Data.Maybe
import           Data.IORef
import           Control.Monad.State
import           Debug.Trace

import           Kvstore.JSONSerialization
import           Kvstore.KVSTypes
import           Kvstore.KeyValueService

import           Kvservice_Types
import qualified DB_Iface               as DB
import           Db_Types

type MockDB = IORef (HM.HashMap T.Text T.Text)

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO DBResponse
  get dbRef key = do
    db <- readIORef dbRef
    return $ DBResponse $ HM.lookup key db

  put :: MockDB -> T.Text -> T.Text -> IO ()
  put dbRef key value = do
    db <- readIORef dbRef
    let convert = \x -> case x of { (Just p) -> p; Nothing -> T.empty }
    let db' = HM.insert key value db
    writeIORef dbRef db'

singleInsert :: Assertion
singleInsert = do
  let req = KVRequest INSERT
                      (T.pack "table-0")
                      (T.pack "key-0")
                      Nothing
                      Nothing
                      $ Just $ (HM.singleton (T.pack "field-0") (T.pack "value-0"))
  let reqs = V.singleton req
  db <- newIORef HM.empty
  (responses, state) <- runStateT (execRequests reqs) $ KVSState HM.empty (db :: MockDB) JSONSerDe
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs state)
  db' <- readIORef $ getDbBackend state
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse INSERT (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"}}")) db'
  assertEqual "cache has wrong data." (HM.singleton (T.pack "table-0") HM.empty) (getKvs state)

main :: IO ()
main = defaultMainWithOpts
       [
        testCase "inserting a value" singleInsert
       ]
       mempty
