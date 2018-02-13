{-# LANGUAGE InstanceSigs, FlexibleInstances #-}
{-# LANGUAGE ImplicitParams #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import qualified Data.HashSet              as Set
import qualified Data.Vector               as V
import           Data.Maybe
import           Data.IORef
import           Control.Monad.State
import           Debug.Trace

import           Kvstore.JSONSerialization
import           Kvstore.KVSTypes
import qualified Kvstore.KeyValueService   as KVS

import           Kvservice_Types
import qualified DB_Iface                  as DB
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

type ExecReqFn = (V.Vector KVRequest) -> StateT (KVSState MockDB JSONSerDe) IO (V.Vector KVResponse)

insertEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> String -> String -> StateT (KVSState MockDB JSONSerDe) IO (V.Vector KVResponse)
insertEntry table key field value = (?execRequests . V.singleton)
                                    $ KVRequest INSERT
                                                (T.pack table)
                                                (T.pack key)
                                                Nothing
                                                Nothing
                                                $ Just $ HM.singleton (T.pack field) (T.pack value)

singleInsert :: (?execRequests :: ExecReqFn) => Assertion
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

deleteEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> StateT (KVSState MockDB JSONSerDe) IO (V.Vector KVResponse)
deleteEntry table key = (?execRequests . V.singleton) $ KVRequest DELETE
                                                                 (T.pack table)
                                                                 (T.pack key)
                                                                 Nothing
                                                                 Nothing
                                                                 Nothing

singleDelete :: (?execRequests :: ExecReqFn) => Assertion
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


updateEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> String -> String -> StateT (KVSState MockDB JSONSerDe) IO (V.Vector KVResponse)
updateEntry table key field value = (?execRequests . V.singleton)
                                    $ KVRequest UPDATE
                                                (T.pack table)
                                                (T.pack key)
                                                Nothing
                                                Nothing
                                                $ Just $ HM.singleton (T.pack field) (T.pack value)

singleUpdate :: (?execRequests :: ExecReqFn) => Assertion
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

readEntry :: (?execRequests :: ExecReqFn)
          => String -> String -> String -> StateT (KVSState MockDB JSONSerDe) IO (V.Vector KVResponse)
readEntry table key field = (?execRequests . V.singleton)
                            $ KVRequest READ
                                        (T.pack table)
                                        (T.pack key)
                                        (Just $ Set.fromList [T.pack field])
                                        Nothing
                                        Nothing

singleRead :: (?execRequests :: ExecReqFn) => Assertion
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

scanEntry :: (?execRequests :: ExecReqFn)
          => String -> String -> String -> StateT (KVSState MockDB JSONSerDe) IO (V.Vector KVResponse)
scanEntry table key field = (?execRequests . V.singleton)
                            $ KVRequest SCAN
                                        (T.pack table)
                                        (T.pack key)
                                        (Just $ Set.fromList [T.pack field])
                                        (Just 3)
                                        Nothing

singleScan :: (?execRequests :: ExecReqFn) => Assertion
singleScan = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                mapM ((\i -> insertEntry ("table-0") ("key-"++i) "field-0" ("value-"++i)) . show) [0,1..5]
                                scanEntry "table-0" "key-1" "field-0"
  -- traceM $ "responses: " ++ show responses
  -- traceM $ "kvs: " ++ (show $ getKvs s')
  db' <- readIORef $ getDbBackend s'
  -- traceM $ "db: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse
                                                  SCAN
                                                  Nothing
                                                  (Just $ V.fromList [ HM.singleton (T.pack "field-0") (T.pack "value-1")
                                                                     , HM.singleton (T.pack "field-0") (T.pack "value-2")
                                                                     , HM.singleton (T.pack "field-0") (T.pack "value-3")
                                                                     ])
                                                  Nothing)

                                responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"},\"key-1\":{\"field-0\":\"value-1\"},\"key-4\":{\"field-0\":\"value-4\"},\"key-5\":{\"field-0\":\"value-5\"},\"key-2\":{\"field-0\":\"value-2\"},\"key-3\":{\"field-0\":\"value-3\"}}")) db'
  assertEqual "cache has wrong data." (HM.singleton (T.pack "table-0")
                                        $ HM.fromList [ ((T.pack "key-0"), HM.singleton (T.pack "field-0") (T.pack "value-0"))
                                                      , ((T.pack "key-1"), HM.singleton (T.pack "field-0") (T.pack "value-1"))
                                                      , ((T.pack "key-2"), HM.singleton (T.pack "field-0") (T.pack "value-2"))
                                                      , ((T.pack "key-3"), HM.singleton (T.pack "field-0") (T.pack "value-3"))
                                                      , ((T.pack "key-4"), HM.singleton (T.pack "field-0") (T.pack "value-4"))
                                                      , ((T.pack "key-5"), HM.singleton (T.pack "field-0") (T.pack "value-5"))
                                                      ])
                                      $ getKvs s'

suite :: (?execRequests :: ExecReqFn) => String -> [Test.Framework.Test]
suite name = [
               testCase "\n=======================================================" (return ())
             , testCase ("*** Running the " ++ name ++ " version: ***") (return ())
             , testCase "inserting a value" singleInsert
             , testCase "deleting a value" singleDelete
             , testCase "updating a value" singleUpdate
             , testCase "reading a value" singleRead
             , testCase "scanning some values" singleScan
             , testCase "=======================================================" (return ())
             ]


main :: IO ()
main = do
  defaultMainWithOpts
    (
       (let ?execRequests = KVS.execRequestsCoarse     in suite "coarse-grained imperative")
    ++ (let ?execRequests = KVS.execRequestsFine       in suite "fine-grained imperative")
    ++ (let ?execRequests = KVS.execRequestsFuncImp    in suite "functional-imperative")
    ++ (let ?execRequests = KVS.execRequestsFunctional in suite "purely functional")
    )
    mempty
