{-# LANGUAGE ImplicitParams #-}

module CorrectnessTests where

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

import           Kvstore.Serialization
import           Kvstore.KVSTypes

import           Kvservice_Types

import           Requests
import           ServiceConfig
import           Versions

initState :: IO (KVSState MockDB)
initState = do
  db <- newIORef HM.empty
  return $ KVSState HM.empty
                    (db :: MockDB)
                    jsonSer
                    jsonDeSer
                    noComp
                    noDecomp
                    undefined
                    undefined

singleInsert :: (?execRequests :: ExecReqFn) => Assertion
singleInsert = do
  s <- initState
  (responses, s') <- flip runStateT s $ insertEntry "table-0" "key-0" "field-0" "value-0"
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs state)
  db' <- readIORef $ _storage s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse INSERT (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"}}")) db'
  assertEqual "cache has wrong data." HM.empty $ _cache s'

singleDelete :: (?execRequests :: ExecReqFn) => Assertion
singleDelete = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                _ <- insertEntry "table-0" "key-0" "field-0" "value-0"
                                deleteEntry "table-0" "key-0"
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs s')
  db' <- readIORef $ _storage s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse DELETE (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0") (T.pack "{}")) db'
  assertEqual "cache has wrong data." HM.empty $ _cache s'

singleUpdate :: (?execRequests :: ExecReqFn) => Assertion
singleUpdate = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                _ <- insertEntry "table-0" "key-0" "field-0" "value-0"
                                updateEntry "table-0" "key-0" "field-0" "value-1"
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs s')
  db' <- readIORef $ _storage s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse UPDATE (Just HM.empty) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-1\"}}")) db'
  assertEqual "cache has wrong data." HM.empty $ _cache s'

singleRead :: (?execRequests :: ExecReqFn) => Assertion
singleRead = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                _ <- insertEntry "table-0" "key-0" "field-0" "value-0"
                                readEntry "table-0" "key-0" "field-0"
  -- traceM $ "responses: " ++ show responses
  -- traceM $ "kvs: " ++ (show $ getKvs s')
  db' <- readIORef $ _storage s'
  -- traceM $ "db: " ++ show db'
  assertEqual "wrong response." (V.singleton $ KVResponse READ (Just $ HM.singleton (T.pack "field-0") (T.pack "value-0")) Nothing Nothing) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"}}")) db'
  assertEqual "cache has wrong data." (HM.singleton (T.pack "table-0")
                                      $ HM.singleton (T.pack "key-0")
                                      $ HM.singleton (T.pack "field-0") (T.pack "value-0"))
                                      $ _cache s'

singleScan :: (?execRequests :: ExecReqFn) => Assertion
singleScan = do
  s <- initState
  (responses, s') <- flip runStateT s $ do
                                mapM_ ((\i -> insertEntry "table-0" ("key-"++i) "field-0" ("value-"++i)) . show) [0,1..5]
                                scanEntry "table-0" "key-1" "field-0"
  -- traceM $ "responses: " ++ show responses
  -- traceM $ "kvs: " ++ (show $ getKvs s')
  db' <- readIORef $ _storage s'
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
                                        $ HM.fromList [ (T.pack "key-0", HM.singleton (T.pack "field-0") (T.pack "value-0"))
                                                      , (T.pack "key-1", HM.singleton (T.pack "field-0") (T.pack "value-1"))
                                                      , (T.pack "key-2", HM.singleton (T.pack "field-0") (T.pack "value-2"))
                                                      , (T.pack "key-3", HM.singleton (T.pack "field-0") (T.pack "value-3"))
                                                      , (T.pack "key-4", HM.singleton (T.pack "field-0") (T.pack "value-4"))
                                                      , (T.pack "key-5", HM.singleton (T.pack "field-0") (T.pack "value-5"))
                                                      ])
                                      $ _cache s'

multipleInserts :: (?execRequests :: ExecReqFn) => Assertion
multipleInserts = do
  s <- initState
  (responses, s') <- flip runStateT s $ insertEntries "table-0" [("key-0","field-0","value-0")
                                                                ,("key-1","field-1","value-1")]
  -- traceM $ "\nresponses: " ++ show responses
  -- traceM $ "\nkvs: " ++ (show $ getKvs state)
  db' <- readIORef $ _storage s'
  -- traceM $ "\ndb: " ++ show db'
  assertEqual "wrong response." (V.fromList  [ KVResponse INSERT (Just HM.empty) Nothing Nothing
                                             , KVResponse INSERT (Just HM.empty) Nothing Nothing]) responses
  assertEqual "db does not contain proper data." (HM.singleton (T.pack "table-0")
                                                               (T.pack "{\"key-0\":{\"field-0\":\"value-0\"},\"key-1\":{\"field-1\":\"value-1\"}}")) db'
  assertEqual "cache has wrong data." HM.empty $ _cache s'

suite :: (?execRequests :: ExecReqFn) => String -> [Test.Framework.Test]
suite name = [
               testCase "inserting a value" singleInsert
             , testCase "deleting a value" singleDelete
             , testCase "updating a value" singleUpdate
             , testCase "reading a value" singleRead
             , testCase "scanning some values" singleScan
             , testCase "multiple inserts in one batch" multipleInserts
             ]

buildSuite = map (\(version, name) -> testGroup name $ let ?execRequests=version in suite name) versions
