{-# LANGUAGE InstanceSigs, FlexibleInstances #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import qualified Data.Text.Lazy         as T
import qualified Data.HashMap.Strict    as HM
import qualified Data.HashSet           as Set
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
    -- return $ DBResponse $ traceShowId $ HM.lookup key db
    return $ DBResponse $ HM.lookup key db

  put :: MockDB -> T.Text -> T.Text -> IO ()
  put dbRef key value = do
    db <- readIORef dbRef
    let convert = \x -> case x of { (Just p) -> p; Nothing -> T.empty }
    let db' = HM.insert key value db
    writeIORef dbRef db'

initState :: IO (KVSState MockDB JSONSerDe)
initState = do
  db <- newIORef HM.empty
  return $ KVSState HM.empty (db :: MockDB) JSONSerDe

insertEntry :: (DB.DB_Iface a, SerDe b) => String -> String -> String -> String -> StateT (KVSState a b) IO (V.Vector KVResponse)
insertEntry table key field value = (execRequests . V.singleton)
                                    $ KVRequest INSERT
                                                (T.pack table)
                                                (T.pack key)
                                                Nothing
                                                Nothing
                                                $ Just $ HM.singleton (T.pack field) (T.pack value)

singleInsert :: Assertion
singleInsert = do
  s <- initState
  (responses, s') <- flip runStateT s $ insertEntry "table-0" "key-0" "field-0" "value-0"
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs state)
  db' <- readIORef $ getDbBackend s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse INSERT (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"}}")) db'
  assertEqual "cache has wrong data." HM.empty $ getKvs s'

deleteEntry :: (DB.DB_Iface a, SerDe b) => String -> String -> StateT (KVSState a b) IO (V.Vector KVResponse)
deleteEntry table key = (execRequests . V.singleton) $ KVRequest DELETE
                                                                 (T.pack table)
                                                                 (T.pack key)
                                                                 Nothing
                                                                 Nothing
                                                                 Nothing

singleDelete :: Assertion
singleDelete = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                _ <- insertEntry "table-0" "key-0" "field-0" "value-0"
                                deleteEntry "table-0" "key-0"
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs s')
  db' <- readIORef $ getDbBackend s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse DELETE (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0") (T.pack "{}")) db'
  assertEqual "cache has wrong data." HM.empty $ getKvs s'


updateEntry :: (DB.DB_Iface a, SerDe b) => String -> String -> String -> String -> StateT (KVSState a b) IO (V.Vector KVResponse)
updateEntry table key field value = (execRequests . V.singleton)
                                    $ KVRequest UPDATE
                                                (T.pack table)
                                                (T.pack key)
                                                Nothing
                                                Nothing
                                                $ Just $ HM.singleton (T.pack field) (T.pack value)

singleUpdate :: Assertion
singleUpdate = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                _ <- insertEntry "table-0" "key-0" "field-0" "value-0"
                                updateEntry "table-0" "key-0" "field-0" "value-1"
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs s')
  db' <- readIORef $ getDbBackend s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse UPDATE (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-1\"}}")) db'
  assertEqual "cache has wrong data." HM.empty $ getKvs s'

readEntry :: (DB.DB_Iface a, SerDe b) => String -> String -> String -> StateT (KVSState a b) IO (V.Vector KVResponse)
readEntry table key field = (execRequests . V.singleton)
                            $ KVRequest READ
                                        (T.pack table)
                                        (T.pack key)
                                        (Just $ Set.fromList [T.pack "field-0"])
                                        Nothing
                                        Nothing

singleRead :: Assertion
singleRead = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                _ <- insertEntry "table-0" "key-0" "field-0" "value-0"
                                readEntry "table-0" "key-0" "field-0"
  -- traceM $ "responses: " ++ show responses
  -- traceM $ "kvs: " ++ (show $ getKvs s')
  db' <- readIORef $ getDbBackend s'
  -- traceM $ "db: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse READ (Just $ HM.singleton (T.pack "field-0") (T.pack "value-0")) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"}}")) db'
  assertEqual "cache has wrong data." (HM.singleton (T.pack "table-0")
                                      $ HM.singleton (T.pack "key-0")
                                      $ HM.singleton (T.pack "field-0") (T.pack "value-0"))
                                      $ getKvs s'

main :: IO ()
main = defaultMainWithOpts
       [
         testCase "inserting a value" singleInsert
       , testCase "deleting a value" singleDelete
       , testCase "updating a value" singleUpdate
       , testCase "reading a value" singleRead
       ]
       mempty
