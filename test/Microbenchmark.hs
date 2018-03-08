{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Microbenchmark where

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import           System.Random
import           Control.Monad.State
import           Control.Lens

import qualified Data.HashMap.Strict       as HM
import qualified Data.HashSet              as Set
import           Data.List
import qualified Data.Vector               as V
import           Data.IORef
import           Data.Time.Clock.POSIX

import           Kvservice_Types
import           Kvstore.KVSTypes

import           Requests
import           TestSetup
import           ServiceConfig

import           Statistics.Sample (mean)

import           Debug.Trace

valueTemplate v = "value-" ++ show v
fieldTemplate f = "field-" ++ show f
keyTemplate k = "key-" ++ show k
tableTemplate t = "table-" ++ show t

data RangeGen = RangeGen Int Int StdGen
instance RandomGen RangeGen where
  next (RangeGen lo hi g) = let (i,g') = randomR (lo,hi) g in (i, RangeGen lo hi g')
  split (RangeGen lo hi g) = let (g1,g2) = split g in (RangeGen lo hi g1, RangeGen lo hi g2)

newtype LinearGen = LinearGen Int
instance RandomGen LinearGen where
  next (LinearGen i) = (i, LinearGen $ i+1)
  split (LinearGen i) = (LinearGen i, LinearGen i)

data BenchmarkState g = BenchmarkState {
                        _fieldCount :: Int,
                        _fieldSelection :: RangeGen,
                        _valueSizeGen :: RangeGen,
                        _tableCount :: Int,
                        _tableSelection :: RangeGen,
                        _keySelection :: g,
                        _operationSelection :: RangeGen,
                        _fieldCountSelection :: RangeGen,
                        _scanCountSelection :: RangeGen
                      }

makeLenses ''BenchmarkState
-- lens does not work with RankNTypes :(

createValue :: forall g. RandomGen g => StateT (BenchmarkState g) IO String
createValue = do
  bmState <- get
  let valSizeGenerator = view valueSizeGen bmState
      (size,valSizeGenerator') = next valSizeGenerator
  put $ over valueSizeGen (const valSizeGenerator') bmState
  (return . concatMap valueTemplate . take size) [1,2..]

createRequest :: forall g. RandomGen g => StateT (BenchmarkState g) IO KVRequest
createRequest = do
  bmState <- get
  let opSelector = view operationSelection bmState
      (op,opSelector') = next opSelector
      bmState' = over operationSelection (const opSelector') bmState

      tableSelector = view tableSelection bmState
      (tableId,tableSelector') = next tableSelector
      table = tableTemplate tableId
      bmState'' = over tableSelection (const tableSelector') bmState'

      keySelector = view keySelection bmState
      (keyId,keySelector') = next keySelector
      key = keyTemplate keyId
      bmState''' = over keySelection (const keySelector') bmState''
  put bmState'''
  case op of
        0 -> prepareINSERT table key <$> createINSERTEntry
        1 -> prepareUPDATE table key <$> createUPDATEEntry
        2 -> prepareREAD table key . Set.map fieldTemplate <$> getFields
        3 -> prepareSCAN table key <$> getScanCount <*> (Set.map fieldTemplate <$> getFields)
        4 -> return $ prepareDELETE table key
        _ -> error $ "No such operation: " ++ show op
    where
      getFieldsAndValues :: forall g. RandomGen g => [Int] -> StateT (BenchmarkState g) IO (HM.HashMap String String)
      getFieldsAndValues = fmap HM.fromList . mapM (\ i -> (,) <$> pure (fieldTemplate i) <*> createValue)
      createINSERTEntry :: forall g. RandomGen g => StateT (BenchmarkState g) IO (HM.HashMap String String)
      createINSERTEntry = getFieldsAndValues . flip take  [1,2..] . view fieldCount =<< get
      getFields :: forall g. RandomGen g => StateT (BenchmarkState g) IO (Set.HashSet Int)
      getFields = do
        s <- get
        let fieldCountGen = view fieldCountSelection s
            (fieldCount,fieldCountGen') = next fieldCountGen
            s' = over fieldCountSelection (const fieldCountGen') s
            fieldSel = view fieldSelection s'
            (fieldSel', fields) = foldl (\ (sel,l) _ -> let (f,sel') = next sel in (sel',l ++ [f]) )
                                       (fieldSel,[])
                                       $ take fieldCount [1,2..]
            s'' = over fieldSelection (const fieldSel') s'
        put s''
        return $ Set.fromList fields
      createUPDATEEntry :: forall g. RandomGen g => StateT (BenchmarkState g) IO (HM.HashMap String String)
      createUPDATEEntry = getFieldsAndValues . Set.toList =<< getFields
      getScanCount = do
        s <- get
        let scanCountSel = view scanCountSelection s
            (scanCount,scanCountSel') = next scanCountSel
        put $ over scanCountSelection (const scanCountSel') s
        return scanCount

workload :: forall g. RandomGen g => Int -> StateT (BenchmarkState g) IO (V.Vector KVRequest)
workload operationCount = V.fromList <$> mapM (const createRequest) [1..operationCount]

showState :: KVSState MockDB -> IO String
showState (KVSState cache dbRef _ _) = do
  db <- readIORef dbRef
  return $ "Cache:\n" ++ show cache ++ "\nDB:\n" ++ show db

loadDB keyCount = do
  s <- initState
  -- fill the db first
  (requests,_) <- runStateT (workload keyCount) $ BenchmarkState
                                            10 -- _fieldCount
                                            (RangeGen 0 10 $ mkStdGen 0) -- _fieldSelection
                                            (RangeGen 5 10 $ mkStdGen 0) -- _valueSizeGen
                                            1 -- _tableCount
                                            (RangeGen 1 1 $ mkStdGen 0) -- _tableSelection
                                            (LinearGen 1) -- _keySelection
                                            (RangeGen 0 0 $ mkStdGen 0) -- _operationSelection (INSERT only)
                                            (RangeGen 3 10 $ mkStdGen 0) -- _fieldCountSelection
                                            (RangeGen 5 10 $ mkStdGen 0) -- _scanCountSelection
  -- traceM "requests (INSERT):"
  -- mapM (\i -> traceM $ show i ++ "\n" ) requests
  (responses, s') <- flip runStateT s $ ?execRequests requests
  responses `seq` assertEqual "wrong number of responses" keyCount $ length responses
  return s'

  -- traceM "state after init:"
  -- traceM =<< showState s'
  -- traceM "done with insert."

reqBenchmarkState keyCount = BenchmarkState
                                10 -- _fieldCount
                                (RangeGen 0 10 $ mkStdGen 0) -- _fieldSelection
                                (RangeGen 5 10 $ mkStdGen 0) -- _valueSizeGen
                                1 -- _tableCount
                                (RangeGen 1 1 $ mkStdGen 0) -- _tableSelection
                                (RangeGen 1 keyCount $ mkStdGen 0) -- _keySelection
                                (RangeGen 1 3 $ mkStdGen 0) -- _operationSelection (no INSERT, no DELETE)
                                (RangeGen 3 10 $ mkStdGen 0) -- _fieldCountSelection
                                (RangeGen 5 10 $ mkStdGen 0) -- _scanCountSelection

currentTimeMillis = round . (* 1000) <$> getPOSIXTime

-- then run some requests
-- runRequests :: (?execRequests :: ExecReqFn)
--             => Int -> Int -> BenchmarkState RangeGen -> KVSState MockDB -> IO (KVSState MockDB, Integer)
runRequests operationCount keyCount bmState s = do
  (requests,_) <- runStateT (workload operationCount) bmState
  -- traceM $ "requests:"
  -- mapM (\i -> traceM $ show i ++ "\n" ) requests
  start <- currentTimeMillis
  (responses, s') <- requests `seq` flip runStateT s $ ?execRequests requests
  stop <- currentTimeMillis
  let execTime = stop - start
  -- traceM "???????????????????????????????????????"
  -- traceM $ "responses:"
  -- mapM (\i -> traceM $ show i ++ "\n" ) responses
  responses `seq` assertEqual "wrong number of responses" operationCount $ length responses
  return (s',execTime)

runSingleBatch :: (?execRequests :: ExecReqFn) => Int -> Int -> IO ()
runSingleBatch keyCount reqCount = do
  (_, execTime) <- runRequests reqCount keyCount (reqBenchmarkState keyCount) =<< loadDB keyCount
  traceM $ "exec time: " ++ show execTime
  return ()

runMultipleBatches :: (?execRequests :: ExecReqFn) => Int -> Int -> Int -> IO ()
runMultipleBatches keyCount reqCount batchCount = do
   s <- loadDB keyCount
   let bmState = reqBenchmarkState keyCount
   (_, execTimes) <- foldM (\ (st,execTimes) _ -> do
                                (st',execTime) <- runRequests reqCount keyCount bmState st
                                return $ st' `seq` (st',execTimes ++ [execTime]))
                           (s,[])
                           $ take batchCount [1,2..]
   let meanExecTime = mean $ V.fromList $ map fromIntegral execTimes
   traceM $ "mean execution time: " ++ show meanExecTime ++ " ms"
   return ()

suite :: (?execRequests :: ExecReqFn) => String -> [Test.Framework.Test]
suite name = [
             -- , testCase "running batch" $ runSingleBatch 2000 20
              testCase "running multiple batches" $ runMultipleBatches 2000 20 20
             ]
